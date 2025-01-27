use crate::core::models::runepool_units_history::RunepoolUnitsInterval;
use crate::utils::metrics::{
    log_db_operation_metrics, DatabaseOperation, DatabaseType, OperationMetrics,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json;
use std::sync::Arc;
use std::time::Instant;

pub async fn get_runepool_units_history_rocksdb(
    db: Arc<rocksdb::DB>,
    limit: u32,
    offset: u32,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    min_units: Option<u64>,
) -> Result<Vec<RunepoolUnitsInterval>> {
    let metrics = OperationMetrics::new(
        DatabaseType::RocksDB,
        DatabaseOperation::Read,
        limit as usize,
        "runepool units".to_string(),
    );

    let start_time_metric = Instant::now();
    let mut results = Vec::new();
    let mut skipped = 0;

    let iter = db.iterator(rocksdb::IteratorMode::Start);

    for item in iter {
        let (_, value) = item?;

        let interval: RunepoolUnitsInterval = serde_json::from_slice(&value)?;

        // Apply filters
        if let (Some(start), Some(end)) = (start_time, end_time) {
            if interval.start_time < start || interval.end_time > end {
                continue;
            }
        }

        if let Some(min_units) = min_units {
            if interval.units <= min_units {
                continue;
            }
        }

        // Handle offset
        if skipped < offset {
            skipped += 1;
            continue;
        }

        results.push(interval);

        if results.len() >= limit as usize {
            break;
        }
    }

    log_db_operation_metrics(
        &format!("read_intervals_{}_records", results.len()),
        start_time_metric,
    );

    metrics.finish();
    Ok(results)
}
