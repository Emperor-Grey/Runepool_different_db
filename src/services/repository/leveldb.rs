use crate::core::models::runepool_units_history::RunepoolUnitsInterval;
use crate::utils::metrics::{DatabaseOperation, DatabaseType, OperationMetrics};
use anyhow::Result;
use std::sync::{Arc, Mutex};

pub async fn store_level_intervals(
    db: Arc<Mutex<rusty_leveldb::DB>>,
    intervals: Vec<RunepoolUnitsInterval>,
) -> Result<(), anyhow::Error> {
    tracing::info!("Starting to store {} intervals in LevelDB", intervals.len());

    let metrics = OperationMetrics::new(
        DatabaseType::LevelDB,
        DatabaseOperation::Write,
        intervals.len(),
        "rune pool history".to_string(),
    );

    let mut db_lock = db
        .lock()
        .map_err(|_| anyhow::anyhow!("Failed to acquire LevelDB lock"))?;

    let mut stored_count = 0;
    for interval in intervals {
        let key = format!(
            "{}:{}",
            interval.start_time.timestamp(),
            interval.end_time.timestamp()
        );

        // Check if record exists
        if db_lock.get(key.as_bytes()).is_none() {
            let value = serde_json::to_vec(&interval)?;
            db_lock.put(key.as_bytes(), &value)?;
            stored_count += 1;
        }
    }

    // Ensure data is written to disk
    db_lock.flush()?;

    tracing::info!(
        "Successfully stored {} new intervals in LevelDB",
        stored_count
    );
    metrics.finish();
    Ok(())
}
