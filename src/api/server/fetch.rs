use super::runepool_units_history::fetch_initial_runepool_units_history;
use crate::services::repository::runepool;

pub async fn fetch_and_store_runepool_units_history() {
    tracing::info!("Fetching initial runepool units history...");
    match fetch_initial_runepool_units_history().await {
        Ok(initial_data) => {
            tracing::info!(
                "Successfully fetched {} intervals from API",
                initial_data.intervals.len()
            );
            tracing::info!("Storing started");
            match runepool::store_intervals(initial_data.intervals).await {
                Ok(_) => tracing::info!("Storage operation completed"),
                Err(e) => tracing::error!("Failed to store intervals: {}", e),
            }
        }
        Err(e) => tracing::error!("Failed to fetch initial runepool units history: {}", e),
    }
}
