use crate::config::connect::DB;
use crate::core::models::runepool_units_history::RunepoolUnitsInterval;
use crate::utils::metrics::{DatabaseOperation, DatabaseType, OperationMetrics};

// Store in SurrealDB
pub async fn store_surreal_intervals(
    intervals: Vec<RunepoolUnitsInterval>,
) -> surrealdb::Result<usize> {
    let metrics = OperationMetrics::new(
        DatabaseType::SurrealDB,
        DatabaseOperation::Write,
        intervals.len(),
        "runepool units".to_string(),
    );

    let mut stored_count = 0;
    for interval in intervals {
        let existing: Option<RunepoolUnitsInterval> = DB
            .query(
                "SELECT * FROM runepool_unit_intervals WHERE startTime = $start AND endTime = $end",
            )
            .bind(("start", interval.start_time))
            .bind(("end", interval.end_time))
            .await?
            .take(0)?;

        if existing.is_none() {
            let _: Option<RunepoolUnitsInterval> = DB
                .create("runepool_unit_intervals")
                .content(interval)
                .await?;
            stored_count += 1;
        }
    }

    metrics.finish();
    Ok(stored_count)
}
