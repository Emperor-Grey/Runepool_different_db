use crate::config::connect::MONGO_CLIENT;
use crate::core::models::runepool_units_history::RunepoolUnitsInterval;
use crate::utils::metrics::{
    log_db_operation_metrics, DatabaseOperation, DatabaseType, OperationMetrics,
};
use anyhow::Result;
use bson::{doc, Document};
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use mongodb::options::FindOptions;
use std::time::Instant;

pub async fn get_runepool_units_history_mongodb(
    limit: u32,
    offset: u32,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    min_units: Option<u64>,
    sort_field: &str,
    sort_order: &str,
) -> Result<Vec<RunepoolUnitsInterval>, mongodb::error::Error> {
    let metrics = OperationMetrics::new(
        DatabaseType::MongoDB,
        DatabaseOperation::Read,
        limit as usize,
        "runepool units".to_string(),
    );

    let client = MONGO_CLIENT.get().expect("MongoDB client not initialized");
    let db = client.database("runepool");
    let collection = db.collection::<Document>("runepool_unit_intervals");

    let mut filter = doc! {};

    if let (Some(start), Some(end)) = (start_time, end_time) {
        filter = doc! {
            "start_time": { "$gte": start },
            "end_time": { "$lte": end }
        };
    }

    if let Some(units) = min_units {
        filter.insert("units", doc! { "$gt": units as i64 });
    }

    let sort_direction = if sort_order.to_lowercase() == "desc" {
        -1
    } else {
        1
    };
    let sort = doc! { sort_field: sort_direction };

    let find_options = FindOptions::builder()
        .sort(sort)
        .skip(offset as u64)
        .limit(limit as i64)
        .build();

    let start_time = Instant::now();
    let mut cursor = collection.find(filter).with_options(find_options).await?;

    let mut results = Vec::new();
    while let Some(doc) = cursor.try_next().await? {
        let interval = RunepoolUnitsInterval {
            start_time: doc
                .get("start_time")
                .unwrap()
                .as_datetime()
                .unwrap()
                .to_chrono(),
            end_time: doc
                .get("end_time")
                .unwrap()
                .as_datetime()
                .unwrap()
                .to_chrono(),
            count: doc.get("count").unwrap().as_i64().unwrap() as u64,
            units: doc.get("units").unwrap().as_i64().unwrap() as u64,
        };
        results.push(interval);
    }

    log_db_operation_metrics(
        &format!("read_intervals_{}_records", results.len()),
        start_time,
    );

    metrics.finish();
    Ok(results)
}
