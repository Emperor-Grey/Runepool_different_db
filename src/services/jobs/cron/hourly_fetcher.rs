use chrono::{DateTime, Duration, Utc};
use sqlx::MySqlPool;
use tokio::time;
use tracing::{error, info};

use crate::services::jobs::cron::runepool_units_history_cron::RunepoolUnitsHistoryCron;

pub struct HourlyFetcher {
    pool: MySqlPool,
    last_run: DateTime<Utc>,
}

impl HourlyFetcher {
    pub fn new(pool: MySqlPool) -> Self {
        Self {
            pool,
            last_run: Utc::now(),
        }
    }

    pub async fn start(&mut self) -> Result<(), anyhow::Error> {
        info!("Starting hourly fetcher...");

        loop {
            let now = Utc::now();
            let duration_since_last = now - self.last_run;

            if duration_since_last >= Duration::hours(1) {
                info!("Starting hourly data fetch cycle...");
                self.last_run = now;

                // Fetch runepool units history
                let runepool_pool = self.pool.clone();
                let mut runepool_cron = RunepoolUnitsHistoryCron::new(runepool_pool);
                if let Err(e) = runepool_cron.fetch_latest_hour().await {
                    error!("Failed to fetch runepool units history: {}", e);
                }

                info!("Completed hourly data fetch cycle");
            }

            // Sleep for a minute before checking again
            time::sleep(Duration::minutes(1).to_std().unwrap()).await;
        }
    }
}
