use crate::audit::AuditGuard;
use crate::config::RelayConfig;
use crate::guards::PipeGuard;

use fjall::Keyspace;
use log::Level;
use rand::RngExt;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;
use tokio::net::windows::named_pipe::ServerOptions;

/// Record encoding in Fjall value:
/// [0..8)   = ingest_ts_ns (u64 BE)
/// [8..12)  = payload_len (u32 BE)
/// [12..]   = payload bytes
const HEADER_LEN: usize = 8 + 4;

/// Ingests data from a Windows Named Pipe and persists it into a Fjall Keyspace.
/// Uses monotonic big-endian u64 sequence number as key for FIFO ordering.
///
/// NOTE:
/// - This implementation is intentionally framing-agnostic. If your producer writes
///   message boundaries that do not align to reads, you should add explicit framing.
pub async fn run_ingestion(pipe_path: String, keyspace: Keyspace, audit: Arc<AuditGuard>) {
    let mut server = ServerOptions::new()
        .first_pipe_instance(true)
        .create(&pipe_path)
        .expect("NIST AC-4: Failed to create secure named pipe");

    // Monotonic sequence number. (If you need crash-stable sequence, persist HWM in Fjall meta.)
    let mut seq_counter: u64 = 0;

    loop {
        if server.connect().await.is_ok() {
            let _g = PipeGuard(&mut server);

            // Consider making this configurable (cfg.ingest.pipe_buffer_size).
            let mut buf = vec![0u8; 65_536];

            while let Ok(n) = _g.0.read(&mut buf).await {
                if n == 0 {
                    break;
                }

                let now_ns = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;

                // Key: big-endian u64 sequence number for FIFO
                let key = seq_counter.to_be_bytes();

                // Value: [timestamp_ns][payload_len][payload_bytes]
                let mut value = Vec::with_capacity(HEADER_LEN + n);
                value.extend_from_slice(&now_ns.to_be_bytes());
                value.extend_from_slice(&(n as u32).to_be_bytes());
                value.extend_from_slice(&buf[..n]);

                if let Err(e) = keyspace.insert(&key, &value) {
                    audit.log(Level::Error, 1022, &format!("Fjall insert failed: {e}"));
                } else {
                    seq_counter = seq_counter.wrapping_add(1);
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Polls the Fjall Keyspace for the oldest batch and attempts to send it to the remote URL.
/// Implements batching, exponential backoff, and audit logging.
///
/// Notes:
/// - HTTP 400: we treat as non-retriable and drop the records (dead-letter TODO).
/// - We send only the payload portion (skipping the header envelope).
pub async fn run_egress(
    url: String,
    http: reqwest::Client,
    keyspace: Keyspace,
    cfg: RelayConfig,
    audit: Arc<AuditGuard>,
) {
    let base_backoff = cfg.forwarder.base_backoff_ms.unwrap_or(500);
    let max_backoff = cfg.forwarder.max_backoff_ms.unwrap_or(30_000);
    let max_jitter = cfg.forwarder.max_jitter_ms.unwrap_or(2_000);
    let batch_size = cfg.forwarder.batch_size.unwrap_or(1_000);

    let mut backoff = base_backoff;
    let mut rng = rand::rng();

    loop {
        // Collect up to batch_size oldest records.
        // Fjall iter yields a Guard; we must extract key/value from it.
        let mut batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(batch_size);

        for guard in keyspace.iter().take(batch_size) {
            match guard.into_inner() {
                Ok((k, v_opt)) => {
                    if let Some(v) = v_opt {
                        batch.push((k.to_vec(), v.to_vec()));
                    }
                }
                Err(e) => {
                    audit.log(Level::Warn, 1031, &format!("Fjall iter read error: {e}"));
                    // continue; do not fail the whole loop
                }
            }
        }

        if batch.is_empty() {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }

        // Build payload by concatenating payload bytes from each record.
        // Stored value includes an envelope header; skip it.
        let mut payload: Vec<u8> = Vec::new();
        for (_k, v) in &batch {
            if v.len() < HEADER_LEN {
                audit.log(Level::Warn, 1031, "Stored record too small to contain header; skipping.");
                continue;
            }
            let declared_len = u32::from_be_bytes([v[8], v[9], v[10], v[11]]) as usize;
            let available = v.len().saturating_sub(HEADER_LEN);
            let take_len = declared_len.min(available);
            payload.extend_from_slice(&v[HEADER_LEN..HEADER_LEN + take_len]);
        }

        // If after filtering we have nothing, avoid sending empty payload; sleep briefly.
        if payload.is_empty() {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        let started = Instant::now();
        match http.post(&url).body(payload).send().await {
            Ok(resp) if resp.status().is_success() => {
                for (k, _v) in &batch {
                    let _ = keyspace.remove(k);
                }

                let latency_ms = started.elapsed().as_millis();
                audit.log(
                    Level::Info,
                    1030,
                    &format!("Batch delivered; count={}; latency={}ms", batch.len(), latency_ms),
                );

                backoff = base_backoff;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }

            Ok(resp) if resp.status().as_u16() == 400 => {
                // Non-retriable payload. In spec: move to dead-letter keyspace.
                // TODO: implement separate dead-letter keyspace and write these records there.
                audit.log(
                    Level::Error,
                    1033,
                    &format!(
                        "Dead-letter (not retried): HTTP 400; dropping batch_size={}",
                        batch.len()
                    ),
                );

                // Drop records to prevent infinite retry loop.
                for (k, _v) in &batch {
                    let _ = keyspace.remove(k);
                }

                backoff = base_backoff;
                tokio::time::sleep(Duration::from_millis(250)).await;
            }

            Ok(resp) => {
                let status = resp.status().as_u16();
                let sleep_ms = backoff + rng.random_range(0..max_jitter);

                audit.log(
                    Level::Warn,
                    1031,
                    &format!("Egress failure HTTP {}; retrying in {}ms", status, sleep_ms),
                );

                tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                backoff = std::cmp::min(backoff.saturating_mul(2), max_backoff);
            }

            Err(e) => {
                let sleep_ms = backoff + rng.random_range(0..max_jitter);

                audit.log(
                    Level::Warn,
                    1031,
                    &format!("Egress failure (transport): {e}; retrying in {}ms", sleep_ms),
                );

                tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                backoff = std::cmp::min(backoff.saturating_mul(2), max_backoff);
            }
        }
    }
}

/// Retention task: periodically enforces max age and max disk size.
///
/// Strategy:
/// - On each tick, delete up to a bounded number of oldest items (FIFO) that exceed max age.
/// - If disk usage exceeds max_disk_bytes, evict oldest items until below threshold or until a cap.
/// This avoids unbounded full scans per tick.
pub async fn run_retention(keyspace: Keyspace, cfg: RelayConfig, audit: Arc<AuditGuard>) {
    let interval = cfg.buffer.retention_check_interval_seconds.unwrap_or(60);
    let max_age_seconds = cfg.buffer.max_age_seconds.unwrap_or(259_200); // 72h default
    let max_disk_bytes = cfg.buffer.max_disk_bytes.unwrap_or(4_294_967_296); // 4GiB default

    // Cap deletions per pass to keep retention from monopolizing CPU/IO.
    let max_deletes_per_pass: usize = 10_000;

    loop {
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let cutoff_ns = now_ns.saturating_sub(max_age_seconds.saturating_mul(1_000_000_000));

        // 1) Age-based eviction (bounded)
        let mut deleted_age = 0usize;

        for guard in keyspace.iter().take(max_deletes_per_pass) {
            let (k, v_opt) = match guard.into_inner() {
                Ok(pair) => pair,
                Err(_) => continue,
            };

            let Some(v) = v_opt else { continue };

            if v.len() < HEADER_LEN {
                // Corrupt/short record; remove it.
                let _ = keyspace.remove(k.as_ref());
                deleted_age += 1;
                if deleted_age >= max_deletes_per_pass {
                    break;
                }
                continue;
            }

            let ts_ns = u64::from_be_bytes([v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7]]);

            if ts_ns < cutoff_ns {
                let _ = keyspace.remove(k.as_ref());
                deleted_age += 1;

                if deleted_age >= max_deletes_per_pass {
                    break;
                }
            } else {
                // Since keys are FIFO, once we reach a not-expired record, we can stop.
                break;
            }
        }

        if deleted_age > 0 {
            audit.log(
                Level::Warn,
                1021,
                &format!("Retention (age): evicted_records={}", deleted_age),
            );
        }

        // 2) Disk-based eviction (bounded)
        let mut deleted_disk = 0usize;
        let mut disk = keyspace.disk_space();

        if disk > max_disk_bytes {
            for guard in keyspace.iter().take(max_deletes_per_pass) {
                let (k, _v_opt) = match guard.into_inner() {
                    Ok(pair) => pair,
                    Err(_) => continue,
                };

                let _ = keyspace.remove(k.as_ref());
                deleted_disk += 1;

                disk = keyspace.disk_space();
                if disk <= max_disk_bytes || deleted_disk >= max_deletes_per_pass {
                    break;
                }
            }

            audit.log(
                Level::Warn,
                1021,
                &format!(
                    "Retention (disk): disk_bytes={} max_disk_bytes={} evicted_records={}",
                    disk, max_disk_bytes, deleted_disk
                ),
            );
        }

        tokio::time::sleep(Duration::from_secs(interval)).await;
    }
}
