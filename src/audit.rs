use winlog; 
use log::Level; // Fixes E0603 [cite: 39]

pub struct AuditGuard {
    source: String,
}

impl AuditGuard {
    pub fn new(source: &str) -> Self {
        // Register the event source with Windows
        let _ = winlog::register(source); 
        Self { source: source.to_string() }
    }

    pub fn log(&self, level: Level, event_id: u32, message: &str) {
        // Log to Windows Event Viewer under 'Application'
        // Fix E0425: Use winlog::report instead of winlog::event_log
        // winlog::report(source_name, level, event_id, &[messages])
        winlog::report(&self.source, level, event_id, &[message]);
    }
}

// RAII: deregister isn't strictly required by Win32, 
// but we maintain the pattern for future-proofing.
impl Drop for AuditGuard {
    fn drop(&mut self) {
        // No-op for winlog 0.3, but hooks exist for advanced cleanup
    }
}
