use crate::guards::PipeGuard;
use crate::audit::AuditGuard;
use crate::config::RelayConfig;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;
use tokio::net::windows::named_pipe::ServerOptions;

// Fjall 3.x: Partition is now called Keyspace
use fjall::Keyspace;

// Fix E0599: Import RngExt to enable random_range 
use rand::{Rng, RngExt}; 
use log::Level;

/// Ingests data from a Windows Named Pipe and persists it into a Fjall 3.x Keyspace.
pub async fn run_ingestion(
    pipe_path: String, 
    keyspace: Keyspace, 
    audit: Arc<AuditGuard>
) {
    let mut server = ServerOptions::new()
        .first_pipe_instance(true)
        .create(&pipe_path)
        .expect("NIST AC-4: Failed to create secure named pipe");

    let mut counter: u64 = 0;

    loop {
        if server.connect().await.is_ok() {
            let _g = PipeGuard(&mut server);
            let mut buf = vec![0; 65536];

            while let Ok(n) = _g.0.read(&mut buf).await {
                if n == 0 { break; }

                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                
                let mut key = [0u8; 16];
                key[..8].copy_from_slice(&now.to_be_bytes());
                key[8..].copy_from_slice(&counter.to_be_bytes());

                // Atomic write to the embedded store
                // Fjall 3.x Keyspace implements insert/remove directly
                if let Err(e) = keyspace.insert(key, &buf[..n]) {
                    audit.log(Level::Error, 502, &format!("Fjall Write Error: {}", e));
                } else {
                    counter = counter.wrapping_add(1);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Polls the Fjall Keyspace for the oldest message and attempts to send it to the remote URL.
pub async fn run_egress(
    url: String, 
    http: reqwest::Client, 
    keyspace: Keyspace, 
    cfg: RelayConfig, 
    audit: Arc<AuditGuard>
) {
    let mut backoff = cfg.base_backoff_ms;
    let mut rng = rand::rng(); 

    loop {
        // In Fjall 3.x, Keyspace provides the iterator directly
        let first_item = keyspace.iter().next();

        if let Some(Ok((key, value))) = first_item {
            match http.post(&url).body(value.to_vec()).send().await {
                Ok(r) if r.status().is_success() => {
                    let _ = keyspace.remove(key);
                    backoff = cfg.base_backoff_ms;
                    tokio::time::sleep(Duration::from_millis(1)).await;
                },
                _ => {
                    let sleep_ms = backoff + rng.random_range(0..cfg.max_jitter_ms);
                    audit.log(Level::Warn, 501, &format!("Egress Failure: Retrying in {}ms", sleep_ms));
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                    backoff = std::cmp::min(backoff * 2, cfg.max_backoff_ms);
                }
            }
        } else {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}
