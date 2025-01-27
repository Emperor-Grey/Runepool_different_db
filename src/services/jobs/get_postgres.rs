use crate::config::connect::DB;
use crate::core::models::runepool_units_history::RunepoolUnitsInterval;
use crate::utils::metrics::{
    log_db_operation_metrics, DatabaseOperation, DatabaseType, OperationMetrics,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::time::Instant;

pub async fn get_runepool_units_history_postgres(
    limit: u32,
    offset: u32,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    min_units: Option<u64>,
    sort_field: &str,
    sort_order: &str,
) -> Result<Vec<RunepoolUnitsInterval>> {
    let metrics = OperationMetrics::new(
        DatabaseType::Postgres,
        DatabaseOperation::Read,
        limit as usize,
        "runepool units".to_string(),
    );

    let mut query = String::from("SELECT * FROM runepool_unit_intervals");
    let mut conditions = Vec::new();

    if let (Some(start), Some(end)) = (start_time, end_time) {
        conditions.push(format!(
            "startTime >= {} AND endTime <= {}",
            start.timestamp(),
            end.timestamp()
        ));
    }

    if let Some(units) = min_units {
        conditions.push(format!("units > {}", units));
    }

    if !conditions.is_empty() {
        query.push_str(" WHERE ");
        query.push_str(&conditions.join(" AND "));
    }

    query.push_str(&format!(" ORDER BY {} {}", sort_field, sort_order));
    query.push_str(&format!(" LIMIT {} START {}", limit, offset));

    let start_time = Instant::now();
    let result: Vec<RunepoolUnitsInterval> = DB.query(&query).await?.take(0)?;
    log_db_operation_metrics(
        &format!("read_intervals_{}_records", result.len()),
        start_time,
    );

    metrics.finish();
    Ok(result)
}
