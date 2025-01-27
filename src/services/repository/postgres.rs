use crate::config::connect::{DB, LEVEL_DB, MONGO_CLIENT, PG_POOL, ROCKS_DB};
use crate::core::models::runepool_units_history::RunepoolUnitsInterval;
use crate::utils::metrics::{
    log_db_operation_metrics, DatabaseOperation, DatabaseType, OperationMetrics,
};
use anyhow::Result;
use bson::{doc, Document};
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use mongodb::options::FindOptions;
use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio;

pub fn convert_datetime(dt: DateTime<Utc>) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(dt.timestamp()).expect("Valid timestamp")
}

pub async fn store_postgres_intervals(
    pool: &PgPool,
    intervals: &[RunepoolUnitsInterval],
) -> sqlx::Result<usize> {
    let metrics = OperationMetrics::new(
        DatabaseType::Postgres,
        DatabaseOperation::Write,
        intervals.len(),
        "runepool units".to_string(),
    );

    let mut stored_count = 0;
    for interval in intervals {
        // Check if record exists
        let exists = sqlx::query!(
            "SELECT COUNT(*) FROM runepool_unit_intervals WHERE start_time = $1 AND end_time = $2",
            convert_datetime(interval.start_time),
            convert_datetime(interval.end_time)
        )
        .fetch_one(pool)
        .await?
        .count
        .unwrap_or(0)
            > 0;

        if !exists {
            sqlx::query!(
                "INSERT INTO runepool_unit_intervals (start_time, end_time, count, units) 
                 VALUES ($1, $2, $3, $4)",
                convert_datetime(interval.start_time),
                convert_datetime(interval.end_time),
                interval.count as i64,
                interval.units as i64
            )
            .execute(pool)
            .await?;
            stored_count += 1;
        }
    }

    metrics.finish();
    Ok(stored_count)
}
