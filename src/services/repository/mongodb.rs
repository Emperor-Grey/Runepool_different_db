use crate::config::connect::MONGO_CLIENT;
use crate::core::models::runepool_units_history::RunepoolUnitsInterval;
use crate::utils::metrics::{DatabaseOperation, DatabaseType, OperationMetrics};
use anyhow::Result;
use bson::{doc, Document};
use chrono::Utc;

// Store in MongoDB
pub async fn store_mongo_intervals(
    intervals: Vec<RunepoolUnitsInterval>,
) -> Result<usize, mongodb::error::Error> {
    let metrics = OperationMetrics::new(
        DatabaseType::MongoDB,
        DatabaseOperation::Write,
        intervals.len(),
        "runepool units".to_string(),
    );

    let client = MONGO_CLIENT.get().expect("MongoDB client not initialized");
    let db = client.database("runepool");
    let collection = db.collection::<Document>("runepool_unit_intervals");

    let mut stored_count = 0;
    for interval in intervals {
        // Check if record exists
        let filter = doc! {
            "start_time": interval.start_time,
            "end_time": interval.end_time
        };

        let exists = collection.find_one(filter.clone()).await?;

        if exists.is_none() {
            let doc = doc! {
                "start_time": interval.start_time,
                "end_time": interval.end_time,
                "count": interval.count as i64,
                "units": interval.units as i64,
                "created_at": Utc::now()
            };

            collection.insert_one(doc).await?;
            stored_count += 1;
        }
    }

    metrics.finish();
    Ok(stored_count)
}
