use crate::core::models::common::{DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE};
use crate::core::models::runepool_units_history::{
    MetaStats, RunepoolUnitsHistoryQueryParams, RunepoolUnitsHistoryResponse,
};
use crate::services::jobs::get_postgres::get_runepool_units_history_postgres;
use axum::http::StatusCode;

use axum::Json;
use axum::{extract::Query, response::IntoResponse};
use serde_json::json;

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

    match get_runepool_units_history_postgres(
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
