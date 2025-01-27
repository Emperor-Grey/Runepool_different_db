use crate::config::connect::{LEVEL_DB, ROCKS_DB};
use crate::core::models::common::{DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE};
use crate::core::models::runepool_units_history::RunepoolUnitsInterval;
use crate::core::models::runepool_units_history::{
    MetaStats, RunepoolUnitsHistoryQueryParams, RunepoolUnitsHistoryResponse,
};
use crate::services::repository::runepool::{self};
use crate::utils::metrics::{
    log_db_operation_metrics, DatabaseOperation, DatabaseType, OperationMetrics,
};
use anyhow::Result;
use axum::http::StatusCode;
use axum::Json;
use axum::{extract::Query, response::IntoResponse};
use chrono::{DateTime, Utc};
use serde_json;
use serde_json::json;
use std::sync::{Arc, Mutex};
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

// LevelDB Implementation
async fn store_level_intervals(
    db: Arc<Mutex<rusty_leveldb::DB>>,
    intervals: Vec<RunepoolUnitsInterval>,
) -> Result<usize, anyhow::Error> {
    let metrics = OperationMetrics::new(
        DatabaseType::LevelDB,
        DatabaseOperation::Write,
        intervals.len(),
        "runepool units".to_string(),
    );

    let mut stored_count = 0;
    let mut db_lock = db
        .lock()
        .map_err(|_| anyhow::anyhow!("Failed to acquire LevelDB lock"))?;

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

    metrics.finish();
    Ok(stored_count)
}

// pub async fn get_runepool_units_history_leveldb(
//     db: Arc<Mutex<rusty_leveldb::DB>>,
//     limit: u32,
//     offset: u32,
//     start_time: Option<DateTime<Utc>>,
//     end_time: Option<DateTime<Utc>>,
//     min_units: Option<u64>,
// ) -> Result<Vec<RunepoolUnitsInterval>> {
//     let metrics = OperationMetrics::new(
//         DatabaseType::LevelDB,
//         DatabaseOperation::Read,
//         limit as usize,
//         "runepool units".to_string(),
//     );

//     let start_time_metric = Instant::now();
//     let mut results = Vec::new();
//     let mut skipped = 0;

//     let db_lock = db
//         .lock()
//         .map_err(|_| anyhow::anyhow!("Failed to acquire LevelDB lock"))?;
//     let iter = db_lock.iter();

//     for item in iter {
//         let (_, value) = item?;

//         let interval: RunepoolUnitsInterval = serde_json::from_slice(&value)?;

//         // Apply filters
//         if let (Some(start), Some(end)) = (start_time, end_time) {
//             if interval.start_time < start || interval.end_time > end {
//                 continue;
//             }
//         }

//         if let Some(min_units) = min_units {
//             if interval.units <= min_units {
//                 continue;
//             }
//         }

//         // Handle offset
//         if skipped < offset {
//             skipped += 1;
//             continue;
//         }

//         results.push(interval);

//         if results.len() >= limit as usize {
//             break;
//         }
//     }

//     log_db_operation_metrics(
//         &format!("read_intervals_{}_records", results.len()),
//         start_time_metric,
//     );

//     metrics.finish();
//     Ok(results)
// }
