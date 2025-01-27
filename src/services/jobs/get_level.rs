use crate::core::models::runepool_units_history::RunepoolUnitsInterval;
use crate::utils::metrics::{
    log_db_operation_metrics, DatabaseOperation, DatabaseType, OperationMetrics,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use rusty_leveldb::LdbIterator;
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub async fn get_runepool_units_history_leveldb(
    db: Arc<Mutex<rusty_leveldb::DB>>,
    limit: u32,
    _offset: u32,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    min_units: Option<u64>,
) -> Result<Vec<RunepoolUnitsInterval>> {
    let metrics = OperationMetrics::new(
        DatabaseType::LevelDB,
        DatabaseOperation::Read,
        limit as usize,
        "runepool units".to_string(),
    );

    let operation_start = Instant::now();
    let mut results = Vec::new();

    // Acquire lock on the database
    let mut db_lock = db
        .lock()
        .map_err(|_| anyhow::anyhow!("Failed to acquire LevelDB lock"))?;

    // Create iterator for the database using new_iter
    let mut iter = db_lock
        .new_iter()
        .map_err(|e| anyhow::anyhow!("Failed to create iterator: {}", e))?;

    // Seek to first record
    iter.seek_to_first();

    // Iterate through the database
    while let Some((_key_bytes, value_bytes)) = iter.next() {
        if results.len() >= limit as usize {
            break;
        }

        // Convert value bytes to Vec<u8> for deserialization
        let value_vec = value_bytes.to_vec();

        // Deserialize the value into RunepoolUnitsInterval
        let interval: RunepoolUnitsInterval = match serde_json::from_slice(&value_vec) {
            Ok(interval) => interval,
            Err(e) => {
                tracing::error!("Failed to deserialize interval from LevelDB: {}", e);
                continue;
            }
        };

        // Apply time range filter if specified
        if let (Some(filter_start), Some(filter_end)) = (start_time, end_time) {
            if interval.start_time < filter_start || interval.end_time > filter_end {
                continue;
            }
        }

        // Apply minimum units filter if specified
        if let Some(min_units) = min_units {
            if interval.units <= min_units {
                continue;
            }
        }

        results.push(interval);
    }

    // Log metrics
    log_db_operation_metrics(
        &format!("read_intervals_{}_records", results.len()),
        operation_start,
    );
    metrics.finish();

    Ok(results)
}
