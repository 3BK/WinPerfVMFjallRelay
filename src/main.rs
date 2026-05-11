mod config; 
mod guards; 
mod tls; 
mod relay; 
mod audit;

use std::{env, sync::Arc, fs, path::Path};
use windows_service::{define_windows_service, service_dispatcher};

// Fjall 3.x Imports
use fjall::{Config, Database, Keyspace};

define_windows_service!(ffi_service_main, my_service_main);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if env::args().any(|x| x == "--console") {
        run_app()?;
    } else {
        service_dispatcher::start("VmRelay", ffi_service_main)?;
    }
    Ok(())
}

fn my_service_main(_args: Vec<std::ffi::OsString>) {
    let _ = run_app();
}

#[tokio::main]
async fn run_app() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration
    let toml_str = fs::read_to_string("config.toml")?;
    let cfg: config::RelayConfig = toml::from_str(&toml_str)?;
    
    // Initialize Audit Logging
    let audit = Arc::new(audit::AuditGuard::new(&cfg.audit_source_name));
    audit.log(log::Level::Info, 1000, "Relay Application Initializing with Fjall v3 storage.");

    // 1. Initialize Fjall v3 Database
    // Fix E0599: Use Config::new and Database::open
    let db_path = Path::new("storage"); 
    let fjall_db = Database::open(Config::new(&cfg.metrics_queue))?;

    // Fix: Open or Create a Keyspace (v3 equivalent of Partition)
    let items: Keyspace = fjall_db.open_keyspace("metrics_queue", Default::default())?;

    // 2. Setup Hardened TLS Client
    let rustls_cfg = tls::build_rustls_config(&cfg.client_cert_sha1);
    let http_client = reqwest::Client::builder()
        .use_preconfigured_tls(rustls_cfg)
        .build()?;

    let audit_ingest = Arc::clone(&audit);
    let pipe_path = cfg.named_pipe_path.clone();
    
    // Keyspace is internally Arced and safe to clone for tasks
    let db_ingest = items.clone();
    let db_egress = items.clone();

    // 3. Spawn Ingestion Task
    tokio::spawn(async move {
        relay::run_ingestion(pipe_path, db_ingest, audit_ingest).await;
    });

    // 4. Run Egress Loop
    relay::run_egress(cfg.pingora_url, http_client, db_egress, cfg, Arc::clone(&audit)).await;

    Ok(())
}
