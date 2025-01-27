use crate::core::models::runepool_units_history::RunepoolUnitsInterval;
use crate::utils::metrics::{DatabaseOperation, DatabaseType, OperationMetrics};
use chrono::{DateTime, Utc};
use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;

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
        let res = sqlx::query!(
            "INSERT INTO runepool_unit_intervals (start_time, end_time, count, units)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (start_time, end_time) DO NOTHING",
            convert_datetime(interval.start_time),
            convert_datetime(interval.end_time),
            interval.count as i64,
            interval.units as i64
        )
        .execute(pool)
        .await?;

        if res.rows_affected() > 0 {
            stored_count += 1;
        }
    }

    metrics.finish();
    Ok(stored_count)
}
