use crate::{
    core::models::{
        common::Interval,
        runepool_units_history::{RunepoolUnitsHistoryParams, RunepoolUnitsHistoryResponse},
    },
    services::client::get_midgard_api_url,
};
use reqwest::Client;

pub async fn fetch_initial_runepool_units_history(
) -> Result<RunepoolUnitsHistoryResponse, reqwest::Error> {
    let client = Client::new();
    let base_url = get_midgard_api_url();

    let params = RunepoolUnitsHistoryParams {
        interval: Some(Interval::Hour),
        count: Some(400),
        from: None,
        to: None,
    };

    let mut url = reqwest::Url::parse(&format!("{}/history/runepool", base_url))
        .expect("Failed to parse URL");

    if let Some(interval) = &params.interval {
        url.query_pairs_mut().append_pair(
            "interval",
            match interval {
                Interval::FiveMin => "5min",
                Interval::Hour => "hour",
                Interval::Day => "day",
                Interval::Week => "week",
                Interval::Month => "month",
                Interval::Quarter => "quarter",
                Interval::Year => "year",
            },
        );
    }

    if let Some(count) = params.count {
        url.query_pairs_mut()
            .append_pair("count", &count.to_string());
    }

    if let Some(from) = params.from {
        url.query_pairs_mut()
            .append_pair("from", &from.timestamp().to_string());
    }

    if let Some(to) = params.to {
        url.query_pairs_mut()
            .append_pair("to", &to.timestamp().to_string());
    }

    let response = client.get(url).send().await?;

    let runepool_units_history = response.json::<RunepoolUnitsHistoryResponse>().await?;
    Ok(runepool_units_history)
}
