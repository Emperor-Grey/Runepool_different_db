use crate::config::connect::{LEVEL_DB, ROCKS_DB};
use crate::core::models::common::{DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE};
use crate::core::models::runepool_units_history::RunepoolUnitsInterval;
use crate::core::models::runepool_units_history::{
    MetaStats, RunepoolUnitsHistoryQueryParams, RunepoolUnitsHistoryResponse,
};
use crate::services::repository::runepool::{self};
use crate::utils::metrics::{
    DatabaseOperation, DatabaseType, OperationMetrics, log_db_operation_metrics,
};
use anyhow::Result;
use axum::Json;
use axum::http::StatusCode;
use axum::{extract::Query, response::IntoResponse};
use chrono::{DateTime, Utc};
use serde_json;
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub async fn _get_runepool_units_history_from_rocksdb(
    Query(params): Query<RunepoolUnitsHistoryQueryParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(DEFAULT_PAGE_SIZE).min(MAX_PAGE_SIZE);
    let offset = params.page.unwrap_or(0) * limit;

    let date_range = params.parse_date_range();
    let _sort_field = params.get_sort_field();
    let _sort_order = if params.order.as_deref() == Some("desc") {
        "DESC"
    } else {
        "ASC"
    };

    let db = ROCKS_DB.get().expect("RocksDB not initialized").clone();

    match runepool::_get_runepool_units_history_rocksdb(
        db,
        limit,
        offset,
        date_range.map(|(start, _)| start),
        date_range.map(|(_, end)| end),
        params.units_gt,
    )
    .await
    {
        Ok(intervals) => {
            if intervals.is_empty() {
                return Json(json!({
                    "success": true,
                    "data": "no data found in the database for the given params"
                }))
                .into_response();
            }

            let meta_stats = MetaStats {
                start_time: intervals[0].start_time,
                end_time: intervals[intervals.len() - 1].end_time,
                start_count: intervals[0].count,
                end_count: intervals[intervals.len() - 1].count,
                start_units: intervals[0].units,
                end_units: intervals[intervals.len() - 1].units,
            };

            Json(RunepoolUnitsHistoryResponse {
                intervals,
                meta_stats,
            })
            .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "success": false,
                "error": format!("Database error: {}", e)
            })),
        )
            .into_response(),
    }
}

// RocksDB Implementation
async fn store_rocks_intervals(
    db: Arc<rocksdb::DB>,
    intervals: Vec<RunepoolUnitsInterval>,
) -> Result<usize, anyhow::Error> {
    let metrics = OperationMetrics::new(
        DatabaseType::RocksDB,
        DatabaseOperation::Write,
        intervals.len(),
        "runepool units".to_string(),
    );

    let mut stored_count = 0;
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
            stored_count += 1;
        }
    }

    metrics.finish();
    Ok(stored_count)
}

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
