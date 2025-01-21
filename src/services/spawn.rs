use super::jobs::cron::runepool_units_history_cron::RunepoolUnitsHistoryCron;

pub fn spawn_cron_jobs() {
    tokio::spawn(async move {
        let mut runepool_cron = RunepoolUnitsHistoryCron::new();
        if let Err(e) = runepool_cron.start().await {
            tracing::error!("Runepool units history cron failed: {}", e);
        }
    });
}
