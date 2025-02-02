use crate::core::models::runepool_units_history::RunepoolUnitsInterval;
use crate::utils::metrics::{DatabaseOperation, DatabaseType, OperationMetrics};
use anyhow::Result;
use std::sync::Arc;

// RocksDB Implementation
pub async fn store_rocks_intervals(
    db: Arc<rocksdb::DB>,
    intervals: Vec<RunepoolUnitsInterval>,
) -> Result<(), anyhow::Error> {
    let metrics = OperationMetrics::new(
        DatabaseType::RocksDB,
        DatabaseOperation::Write,
        intervals.len(),
        "runepool units".to_string(),
    );

    for interval in intervals {
        let key = format!(
            "{}:{}",
            interval.start_time.timestamp(),
            interval.end_time.timestamp()
        );

        // Check if record exists
        if db.get(key.as_bytes())?.is_none() {
            let value = serde_json::to_vec(&interval)?;
            db.put(key.as_bytes(), value)?;
        }
    }

    // Ensure data is written to disk
    db.flush()?;

    metrics.finish();
    Ok(())
}
