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

use super::mongodb::store_mongo_intervals;
use super::postgres::store_postgres_intervals;
use super::surrealdb::store_surreal_intervals;

pub async fn store_rocks_intervals(
    db: Arc<rocksdb::DB>,
    intervals: Vec<RunepoolUnitsInterval>,
) -> Result<(), anyhow::Error> {
    let metrics = OperationMetrics::new(
        DatabaseType::RocksDB,
        DatabaseOperation::Write,
        intervals.len(),
        "runepool units".to_string(),
    );

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
        }
    }

    // Ensure data is written to disk
    db.flush()?;

    metrics.finish();
    Ok(())
}

async fn store_level_intervals(
    _db: Arc<Mutex<rusty_leveldb::DB>>,
    _intervals: Vec<RunepoolUnitsInterval>,
) -> Result<(), anyhow::Error> {
    // ... implementation ...
    Ok(())
}

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

// Main store function that coordinates both storage operations
pub async fn store_intervals(intervals: Vec<RunepoolUnitsInterval>) -> Result<(), anyhow::Error> {
    let intervals_clone1 = intervals.clone();
    let intervals_clone2 = intervals.clone();
    let intervals_clone3 = intervals.clone();
    let intervals_clone4 = intervals.clone();
    let intervals_clone5 = intervals;

    let pg_task = tokio::spawn(async move {
        if let Some(pool) = PG_POOL.get() {
            match store_postgres_intervals(pool, &intervals_clone1).await {
                Ok(_) => {
                    tracing::info!("Successfully stored intervals in PostgreSQL");
                    Ok(())
                }
                Err(e) => {
                    tracing::error!("Failed to store in PostgreSQL: {}", e);
                    Err(anyhow::anyhow!("PostgreSQL storage failed: {}", e))
                }
            }
        } else {
            Ok(())
        }
    });

    let surreal_task = tokio::spawn(async move {
        match store_surreal_intervals(intervals_clone2).await {
            Ok(_) => {
                tracing::info!("Successfully stored intervals in SurrealDB");
                Ok(())
            }
            Err(e) => {
                tracing::error!("Failed to store in SurrealDB: {}", e);
                Err(anyhow::anyhow!("SurrealDB storage failed: {}", e))
            }
        }
    });

    let mongo_task = tokio::spawn(async move {
        match store_mongo_intervals(intervals_clone5).await {
            Ok(_) => {
                tracing::info!("Successfully stored intervals in MongoDB");
                Ok(())
            }
            Err(e) => {
                tracing::error!("Failed to store in MongoDB: {}", e);
                Err(anyhow::anyhow!("MongoDB storage failed: {}", e))
            }
        }
    });

    let rocks_task = tokio::spawn(async move {
        if let Some(db) = ROCKS_DB.get() {
            match store_rocks_intervals(db.clone(), intervals_clone3).await {
                Ok(()) => {
                    tracing::info!("Successfully stored intervals in RocksDB");
                    Ok(())
                }
                Err(e) => {
                    tracing::error!("Failed to store in RocksDB: {}", e);
                    Err(anyhow::anyhow!("RocksDB storage failed: {}", e))
                }
            }
        } else {
            Ok(())
        }
    });

    let level_task = tokio::spawn(async move {
        if let Some(db) = LEVEL_DB.get() {
            match store_level_intervals(db.clone(), intervals_clone4).await {
                Ok(_) => {
                    tracing::info!("Successfully stored intervals in LevelDB");
                    Ok(())
                }
                Err(e) => {
                    tracing::error!("Failed to store in LevelDB: {}", e);
                    Err(anyhow::anyhow!("LevelDB storage failed: {}", e))
                }
            }
        } else {
            Ok(())
        }
    });

    let (pg_result, surreal_result, rocks_result, level_result, mongo_result) =
        tokio::join!(pg_task, surreal_task, rocks_task, level_task, mongo_task);

    // Handle results
    match (
        pg_result,
        surreal_result,
        rocks_result,
        level_result,
        mongo_result,
    ) {
        (Ok(Ok(())), Ok(Ok(())), Ok(Ok(())), Ok(Ok(())), Ok(Ok(()))) => Ok(()),
        _ => Err(anyhow::anyhow!("One or more storage operations failed")),
    }
}

pub async fn get_runepool_units_history_surrealdb(
    limit: u32,
    offset: u32,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    min_units: Option<u64>,
    sort_field: &str,
    sort_order: &str,
) -> Result<Vec<RunepoolUnitsInterval>> {
    let metrics = OperationMetrics::new(
        DatabaseType::SurrealDB,
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

pub async fn get_runepool_units_history_rocksdb(
    db: Arc<rocksdb::DB>,
    limit: u32,
    _offset: u32,
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

    let mut results = Vec::new();
    let iter = db.iterator(rocksdb::IteratorMode::Start);

    for result in iter {
        if results.len() >= limit as usize {
            break;
        }

        let (_key, value) = result?;
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

        results.push(interval);
    }

    metrics.finish();
    Ok(results)
}
