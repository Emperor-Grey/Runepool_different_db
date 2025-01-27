use crate::config::connect::{LEVEL_DB, PG_POOL, ROCKS_DB};
use crate::core::models::runepool_units_history::RunepoolUnitsInterval;
use anyhow::Result;
use tokio;

use super::leveldb::store_level_intervals;
use super::mongodb::store_mongo_intervals;
use super::postgres::store_postgres_intervals;
use super::rocksdb::store_rocks_intervals;
use super::surrealdb::store_surreal_intervals;

// Main store function that coordinates all storage operations
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
            tracing::info!("might be rocks db not initialized");
            Ok(()) // This silently succeeds when RocksDB is not initialized!
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
