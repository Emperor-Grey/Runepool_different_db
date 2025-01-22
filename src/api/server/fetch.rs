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

// pub async fn _fetch_and_store_initial_data() -> Result<(), anyhow::Error> {
//     tracing::info!("Fetching initial 400 records...");

//     let client = reqwest::Client::new();
//     let base_url = get_midgard_api_url();

//     let params = RunepoolUnitsHistoryParams {
//         interval: Some(Interval::Hour),
//         count: Some(400),
//         from: None,
//         to: None,
//     };

//     let mut url = reqwest::Url::parse(&format!("{}/history/runepool", base_url))?;

//     if let Some(interval) = &params.interval {
//         url.query_pairs_mut()
//             .append_pair("interval", &interval.to_string());
//     }

//     if let Some(count) = params.count {
//         url.query_pairs_mut()
//             .append_pair("count", &count.to_string());
//     }

//     if let Some(to) = params.to {
//         url.query_pairs_mut()
//             .append_pair("to", &to.timestamp().to_string());
//     }

//     let response = client.get(url.clone()).send().await?;
//     let runepool_history = response.json::<RunepoolUnitsHistoryResponse>().await?;

//     tracing::info!(
//         "Successfully fetched {} intervals",
//         runepool_history.intervals.len()
//     );

//     match runepool::store_intervals(runepool_history.intervals).await {
//         Ok(_) => {
//             tracing::info!("Successfully stored intervals in all databases");
//             Ok(())
//         }
//         Err(e) => {
//             tracing::error!("Failed to store intervals: {}", e);
//             Err(e)
//         }
//     }
// }
