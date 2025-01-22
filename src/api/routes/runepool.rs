use crate::config::connect::ROCKS_DB;
use crate::core::models::common::{DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE};
use crate::core::models::runepool_units_history::{
    MetaStats, RunepoolUnitsHistoryQueryParams, RunepoolUnitsHistoryResponse, RunepoolUnitsInterval,
};
use crate::utils::metrics::{DatabaseOperation, DatabaseType, OperationMetrics};
use anyhow::{anyhow, Result};
use axum::http::StatusCode;
use chrono::{DateTime, Utc};

use crate::services::repository::runepool;
use axum::Json;
use axum::{extract::Query, response::IntoResponse};
use serde_json::json;

pub async fn get_runepool_units_history_from_surrealdb(
    Query(params): Query<RunepoolUnitsHistoryQueryParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(DEFAULT_PAGE_SIZE).min(MAX_PAGE_SIZE);
    let offset = params.page.unwrap_or(0) * limit;

    let date_range = params.parse_date_range();
    let sort_field = params.get_sort_field();
    let sort_order = if params.order.as_deref() == Some("desc") {
        "DESC"
    } else {
        "ASC"
    };

    match runepool::get_runepool_units_history_surrealdb(
        limit,
        offset,
        date_range.map(|(start, _)| start),
        date_range.map(|(_, end)| end),
        params.units_gt,
        sort_field,
        sort_order,
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

pub async fn get_runepool_units_history_from_postgres(
    Query(params): Query<RunepoolUnitsHistoryQueryParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(DEFAULT_PAGE_SIZE).min(MAX_PAGE_SIZE);
    let offset = params.page.unwrap_or(0) * limit;

    let date_range = params.parse_date_range();
    let sort_field = params.get_sort_field();
    let sort_order = if params.order.as_deref() == Some("desc") {
        "DESC"
    } else {
        "ASC"
    };

    match runepool::get_runepool_units_history_postgres(
        limit,
        offset,
        date_range.map(|(start, _)| start),
        date_range.map(|(_, end)| end),
        params.units_gt,
        sort_field,
        sort_order,
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

// pub async fn get_runepool_units_history_from_rocksdb(
//     start_time: Option<DateTime<Utc>>,
//     end_time: Option<DateTime<Utc>>,
//     limit: u32,
//     min_units: Option<u64>,
//     sort_field: &str,
//     sort_order: &str,
// ) -> IntoResponse {
//     let metrics = OperationMetrics::new(
//         DatabaseType::RocksDB,
//         DatabaseOperation::Read,
//         limit as usize,
//         "runepool units".to_string(),
//     );

//     let db = ROCKS_DB
//         .get()
//         .ok_or_else(|| anyhow!("RocksDB not initialized"))?;
//     let mut intervals = Vec::new();

//     // Iterate through RocksDB entries
//     let iter = db.iterator(rocksdb::IteratorMode::Start);

//     for item in iter {
//         let (key, value) = item.map_err(|e| anyhow!("RocksDB iteration error: {}", e))?;

//         let interval: RunepoolUnitsInterval = serde_json::from_slice(&value)
//             .map_err(|e| anyhow!("Failed to deserialize interval: {}", e))?;

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

//         intervals.push(interval);
//     }

//     // Sort results
//     intervals.sort_by(|a, b| {
//         let cmp = match sort_field {
//             "units" => a.units.cmp(&b.units),
//             "count" => a.count.cmp(&b.count),
//             _ => a.start_time.cmp(&b.start_time),
//         };

//         if sort_order == "desc" {
//             cmp.reverse()
//         } else {
//             cmp
//         }
//     });

//     // Apply limit
//     intervals.truncate(limit as usize);

//     metrics.finish();
//     Ok(intervals)
// }

pub async fn get_runepool_units_history_from_rocksdb(
    Query(params): Query<RunepoolUnitsHistoryQueryParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(DEFAULT_PAGE_SIZE).min(MAX_PAGE_SIZE);
    let offset = params.page.unwrap_or(0) * limit;

    let date_range = params.parse_date_range();
    let sort_field = params.get_sort_field();
    let sort_order = if params.order.as_deref() == Some("desc") {
        "DESC"
    } else {
        "ASC"
    };

    let db = ROCKS_DB.get().expect("RocksDB not initialized").clone();

    match runepool::get_runepool_units_history_rocksdb(
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
