use crate::config::connect::DB;
use crate::core::models::runepool_units_history::RunepoolUnitsInterval;
use chrono::{DateTime, Utc};
use surrealdb::Result;

pub async fn store_intervals(intervals: Vec<RunepoolUnitsInterval>) -> Result<()> {
    let mut stored_count = 0;
    for interval in intervals {
        tracing::info!("Storing interval: {:?}", interval);
        // Create a query to check if record exists
        let existing: Option<RunepoolUnitsInterval> = DB
            .query(
                "SELECT * FROM runepool_unit_intervals WHERE startTime = $start AND endTime = $end",
            )
            .bind(("start", interval.start_time))
            .bind(("end", interval.end_time))
            .await?
            .take(0)?;

        if existing.is_none() {
            let new_interval = RunepoolUnitsInterval {
                id: None, // SurrealDB will auto generate it
                count: interval.count,
                end_time: interval.end_time,
                start_time: interval.start_time,
                units: interval.units,
            };

            // Insert new record if it doesn't exist
            let _: Option<RunepoolUnitsInterval> = DB
                .create("runepool_unit_intervals")
                .content(new_interval)
                .await?;

            stored_count += 1;
        }
    }

    tracing::info!("Successfully stored {} new records", stored_count);
    Ok(())
}

pub async fn get_runepool_units_history(
    limit: u32,
    offset: u32,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    min_units: Option<u64>,
    sort_field: &str,
    sort_order: &str,
) -> Result<Vec<RunepoolUnitsInterval>> {
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

    let result: Vec<RunepoolUnitsInterval> = DB.query(&query).await?.take(0)?;

    Ok(result)
}
