#![allow(unused, dead_code)]

use api::{
    routes::runepool::get_runepool_units_history,
    server::fetch::fetch_and_store_runepool_units_history,
};
use axum::{routing::get, Router};
use chrono::Utc;
use config::connect::connect_db;
use dotenv::dotenv;
use http::Method;
use services::spawn::spawn_cron_jobs;
use services::{client::get_midgard_api_url, jobs::cron::hourly_fetcher::HourlyFetcher};
use std::io::Write;
use std::net::SocketAddr;
use std::{env, fs::OpenOptions};
use tokio::{net::TcpListener, time::Instant};
use tower_http::cors::{Any, CorsLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod api;
mod config;
mod core;
mod services;

// /* ************************************************************ */
// /* ************************************************************ */
// /* !NOTE: PLEASE FETCH THINGS ONE BY ONE BECAUSE OF RATE LIMITS */
// /* ************************************************************ */
// /* ************************************************************ */
// #[tokio::main(flavor = "multi_thread", worker_threads = 10)]
// async fn main() {
//     dotenv().ok();

//     let database_url = std::env::var("DATABASE_URL").expect("Database url issue");

//     tracing::info!(
//         "Env variables are \n{}\n{}",
//         get_midgard_api_url(),
//         database_url
//     );

//     // let pool = connect::connect_database(&database_url)
//     //     .await
//     //     .expect("Failed to connect to database");

//     let pool = connect_db().await.unwrap();

//     setup_tracing();

//     tracing::info!("Connected to database...");
//     println!("Current Utc TimeStamp: {:?}", Utc::now().timestamp());

//     // !NOTE: Uncomment this if you want to fetch initial data and read the comment above the main
//     // spawn_cron_jobs(pool.clone());
//     // fetch_initial_data(pool.clone()).await;

//     let hourly_pool = pool.clone();
//     tokio::spawn(async move {
//         let mut hourly_fetcher = HourlyFetcher::new(hourly_pool);
//         if let Err(e) = hourly_fetcher.start().await {
//             tracing::error!("Hourly fetcher failed: {}", e);
//         }
//     });

//     start_server(pool).await;
// }

#[tokio::main]
async fn main() {
    dotenv().ok();
    setup_tracing();

    connect_db().await.expect("Failed to connect to database");

    tracing::info!(
        "Env variables are \n{}\n{}",
        get_midgard_api_url(),
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set")
    );

    println!("Current Utc TimeStamp: {:?}", Utc::now().timestamp());

    // !NOTE: Uncomment this if you want to fetch initial data and read the comment above the main
    // spawn_cron_jobs();
    // fetch_initial_data().await;

    tokio::spawn(async move {
        let mut hourly_fetcher = HourlyFetcher::new();
        if let Err(e) = hourly_fetcher.start().await {
            tracing::error!("Hourly fetcher failed: {}", e);
        }
    });

    start_server().await;
}

fn setup_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| format!("{}=info", env!("CARGO_CRATE_NAME")).into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}

async fn fetch_initial_data() {
    tracing::info!("Starting initial data fetch...");

    fetch_and_store_runepool_units_history().await;
}

async fn start_server() {
    let app = Router::new()
        .layer(CorsLayer::new().allow_origin(Any).allow_methods([
            Method::GET,
            Method::PUT,
            Method::POST,
            Method::DELETE,
        ]))
        .route("/runepool_units_history", get(get_runepool_units_history));

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let listener = TcpListener::bind(addr).await.unwrap();

    tracing::debug!("listening on {}", listener.local_addr().unwrap());

    axum::serve(listener, app.into_make_service())
        .await
        .unwrap();
}

pub fn performance_metrics(start_time: Instant, end_time: Instant, message: &str) {
    let duration = end_time.duration_since(start_time);

    let seconds = duration.as_secs();
    let milliseconds = duration.subsec_millis();
    let minutes = seconds / 60;
    let seconds = seconds % 60;

    let metrics_message = format!("{} {}m {}s {}ms\n", message, minutes, seconds, milliseconds);

    println!("{}", metrics_message);

    write_metrics_into_file(metrics_message);
}

pub fn write_metrics_into_file(metrics_message: String) {
    let file_path = "performance-metrics.txt";

    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(file_path)
        .expect("Failed to open the file for writing");

    if let Err(e) = file.write_all(metrics_message.as_bytes()) {
        eprintln!("Error writing to file: {}", e);
    }
}
