use crate::guards::PipeGuard;
use crate::audit::AuditGuard;
use crate::config::RelayConfig;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;
use tokio::net::windows::named_pipe::ServerOptions;
use fjall::{Config, Keyspace, PartitionHandle};
use rand::Rng;

pub async fn run_ingestion(
    pipe_path: String, 
    db_partition: PartitionHandle, 
    audit: Arc<AuditGuard>
) {
    let mut server = ServerOptions::new()
        .first_pipe_instance(true)
        .create(&pipe_path)
        .expect("NIST AC-4: Failed to create secure pipe");

    let mut counter: u64 = 0;

    loop {
        if server.connect().await.is_ok() {
            let _g = PipeGuard(&mut server);
            let mut buf = vec![0; 65536];

            while let Ok(n) = _g.0.read(&mut buf).await {
                if n == 0 { break; }

                // Generate a monotonic key: [timestamp_ms (8b)][counter (8b)]
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                
                let mut key = [0u8; 16];
                key[..8].copy_from_slice(&now.to_be_bytes());
                key[8..].copy_from_slice(&counter.to_be_bytes());

                // Atomic write to LSM-tree
                if let Err(e) = db_partition.insert(key, &buf[..n]) {
                    audit.log(log::Level::Error, 502, &format!("Fjall Write Error: {}", e));
                } else {
                    counter += 1;
                }
            }
        }
    }
}

pub async fn run_egress(
    url: String, 
    http: reqwest::Client, 
    db_partition: PartitionHandle, 
    cfg: RelayConfig, 
    audit: Arc<AuditGuard>
) {
    let mut backoff = cfg.base_backoff_ms;
    let mut rng = rand::rng(); 

    loop {
        // Retrieve the oldest item (first in the LSM tree)
        let first_item = db_partition.iter().next();

        if let Some(Ok((key, value))) = first_item {
            match http.post(&url).body(value.to_vec()).send().await {
                Ok(r) if r.status().is_success() => {
                    // Success: Remove from persistent store
                    let _ = db_partition.remove(key);
                    backoff = cfg.base_backoff_ms;
                    
                    // Throttling to prevent CPU spinning
                    tokio::time::sleep(Duration::from_millis(10)).await;
                },
                _ => {
                    let sleep = backoff + rng.random_range(0..cfg.max_jitter_ms);
                    audit.log(log::Level::Warn, 501, &format!("Egress Failure: Backing off {}ms", sleep));
                    tokio::time::sleep(Duration::from_millis(sleep)).await;
                    backoff = std::cmp::min(backoff * 2, cfg.max_backoff_ms);
                }
            }
        } else {
            // Queue is empty: Wait for new data
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}
