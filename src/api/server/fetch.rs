use super::runepool_units_history::fetch_initial_runepool_units_history;
use crate::services::repository::runepool;

pub async fn fetch_and_store_runepool_units_history(pool: &sqlx::MySqlPool) {
    tracing::info!("Fetching initial runepool units history...");
    match fetch_initial_runepool_units_history().await {
        Ok(initial_data) => {
            tracing::info!("Successfully fetched initial runepool units history");
            match runepool::store_intervals(pool, &initial_data.intervals).await {
                Ok(_) => tracing::info!(
                    "Successfully stored {} intervals",
                    initial_data.intervals.len()
                ),
                Err(e) => tracing::error!("Failed to store intervals: {}", e),
            }
        }
        Err(e) => tracing::error!("Failed to fetch initial runepool units history: {}", e),
    }
}
