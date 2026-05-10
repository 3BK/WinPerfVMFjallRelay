mod config; mod guards; mod tls; mod relay; mod audit;

use std::{env, sync::Arc};
use windows_service::{define_windows_service, service_dispatcher};
use crate::audit::AuditGuard;

define_windows_service!(ffi_service_main, my_service_main);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if env::args().any(|x| x == "--console") {
        run_app()?;
    } else {
        service_dispatcher::start("VmIggyRelay", ffi_service_main)?;
    }
    Ok(())
}

fn my_service_main(_args: Vec<std::ffi::OsString>) {
    let _ = run_app();
}

#[tokio::main]
async fn run_app() -> Result<(), Box<dyn std::error::Error>> {
    let cfg: config::RelayConfig = toml::from_str(&std::fs::read_to_string("config.toml")?)?;
    
    // Initialize Audit Logging (Windows Event Log)
    let audit = Arc::new(AuditGuard::new(&cfg.audit_source_name));
    audit.log(winlog::Level::Info, 1000, "Relay Application Initializing.");

    // Setup Hardened Client
    let rustls_cfg = tls::create_rustls_config(&cfg.client_cert_sha1, &cfg.server_sha256_pin);
    let http_client = reqwest::Client::builder()
        .use_preconfigured_tls(rustls_cfg)
        .build()?;

    // Connect Persistence (Iggy)
    let mut iggy = iggy::client_provider::ClientProvider::get_default_client().await?;
    iggy.connect().await?;

    let audit_ingest = Arc::clone(&audit);
    let iggy_ingest = iggy.clone();
    let pipe_path = cfg.named_pipe_path.clone();
    let s_id = cfg.iggy_stream_id;
    let t_id = cfg.iggy_topic_id;

    tokio::spawn(async move {
        relay::run_ingestion(pipe_path, s_id, t_id, iggy_ingest, audit_ingest).await;
    });

    relay::run_egress(cfg.pingora_url, http_client, cfg, Arc::clone(&audit)).await;

    Ok(())
}
