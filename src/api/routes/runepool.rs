use crate::services::handlers::{
    leveldb::get_runepool_units_history_from_leveldb,
    mongodb::get_runepool_units_history_from_mongodb,
    postgres::get_runepool_units_history_from_postgres,
    rocksdb::get_runepool_units_history_from_rocksdb,
    surrealdb::get_runepool_units_history_from_surrealdb,
};
use axum::{routing::get, Router};
use http::Method;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};


pub async fn start_server() {
    let app = Router::new()
        .layer(CorsLayer::new().allow_origin(Any).allow_methods([
            Method::GET,
            Method::PUT,
            Method::POST,
            Method::DELETE,
        ]))
        .route(
            "/runepool/surrealdb",
            get(get_runepool_units_history_from_surrealdb),
        )
        .route(
            "/runepool/postgres",
            get(get_runepool_units_history_from_postgres),
        )
        .route(
            "/runepool/mongo",
            get(get_runepool_units_history_from_mongodb),
        )
        .route(
            "/runepool/rocks",
            get(get_runepool_units_history_from_rocksdb),
        )
        .route(
            "/runepool/level",
            get(get_runepool_units_history_from_leveldb),
        );

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let listener = TcpListener::bind(addr).await.unwrap();

    tracing::debug!("listening on {}", listener.local_addr().unwrap());

    axum::serve(listener, app.into_make_service())
        .await
        .unwrap();
}
