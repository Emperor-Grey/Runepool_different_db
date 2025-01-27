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

// RocksDB Implementation
async fn store_rocks_intervals(
    db: Arc<rocksdb::DB>,
    intervals: Vec<RunepoolUnitsInterval>,
) -> Result<usize, anyhow::Error> {
    let metrics = OperationMetrics::new(
        DatabaseType::RocksDB,
        DatabaseOperation::Write,
        intervals.len(),
        "runepool units".to_string(),
    );

    let mut stored_count = 0;
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
            stored_count += 1;
        }
    }

    metrics.finish();
    Ok(stored_count)
}
