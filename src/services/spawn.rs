use super::jobs::cron::runepool_units_history_cron::RunepoolUnitsHistoryCron;

pub fn spawn_cron_jobs(pool: sqlx::MySqlPool) {
    let runepool_pool = pool.clone();
    tokio::spawn(async move {
        let mut runepool_cron = RunepoolUnitsHistoryCron::new(runepool_pool);
        if let Err(e) = runepool_cron.start().await {
            tracing::error!("Runepool units history cron failed: {}", e);
        }
    });
}
