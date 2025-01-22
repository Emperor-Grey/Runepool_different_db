use super::jobs::cron::runepool_units_history_cron::_RunepoolUnitsHistoryCron;

pub fn _spawn_cron_jobs() {
    tokio::spawn(async move {
        let mut runepool_cron = _RunepoolUnitsHistoryCron::_new();
        if let Err(e) = runepool_cron._start().await {
            tracing::error!("Runepool units history cron failed: {}", e);
        }
    });
}
