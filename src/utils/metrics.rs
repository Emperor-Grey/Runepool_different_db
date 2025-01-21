use chrono::Utc;
use std::fs::OpenOptions;
use std::io::Write;
use std::time::Instant;
use tracing::info;

pub enum DatabaseOperation {
    Read,
    Write,
}

pub enum DatabaseType {
    MongoDB,
    Postgres,
    SurrealDB,
    LevelDB,
    RocksDB,
}

pub struct OperationMetrics {
    db_type: DatabaseType,
    operation: DatabaseOperation,
    record_count: usize,
    data_type: String,
    start_time: Instant,
}

impl OperationMetrics {
    pub fn new(
        db_type: DatabaseType,
        operation: DatabaseOperation,
        record_count: usize,
        data_type: String,
    ) -> Self {
        Self {
            db_type,
            operation,
            record_count,
            data_type,
            start_time: Instant::now(),
        }
    }

    pub fn finish(self) {
        let duration = self.start_time.elapsed();
        let operation_type = match self.operation {
            DatabaseOperation::Read => "read",
            DatabaseOperation::Write => "write",
        };

        let db_name = match self.db_type {
            DatabaseType::MongoDB => "MongoDB",
            DatabaseType::Postgres => "Postgres",
            DatabaseType::SurrealDB => "SurrealDB",
            DatabaseType::LevelDB => "LevelDB",
            DatabaseType::RocksDB => "RocksDB",
        };

        let message = format!(
            "Time taken for {} to {} {} data ({} records) : {}m {}s {}ms\n",
            db_name,
            operation_type,
            self.data_type,
            self.record_count,
            duration.as_secs() / 60,
            duration.as_secs() % 60,
            duration.subsec_millis()
        );

        if let Err(e) = write_to_metrics_file(&message) {
            tracing::error!("Failed to write metrics to file: {}", e);
        }
    }
}

fn write_to_metrics_file(message: &str) -> std::io::Result<()> {
    let file_path = "performance_metrics.txt";
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_path)?;

    // Add timestamp to the message
    let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S");
    writeln!(file, "[{}] {}", timestamp, message)?;
    Ok(())
}

pub fn log_db_operation_metrics(operation: &str, start_time: Instant) {
    let duration = start_time.elapsed();
    info!(
        "Database Operation: {}, Duration: {:.2?}",
        operation, duration
    );
}
