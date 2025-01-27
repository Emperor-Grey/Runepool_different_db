use db_tester::{
    api::{routes::runepool::start_server, server::fetch::fetch_and_store_initial_data},
    config::{
        connect::{
            connect_db, connect_leveldb, connect_mongodb, connect_rocksdb, initialize_pg_pool,
        },
        tracing::setup_tracing,
    },
    services::client::get_midgard_api_url,
};
use dotenv::dotenv;

#[tokio::main]
async fn main() {
    dotenv().ok();
    setup_tracing();

    tracing::info!(
        "Env variables are \n{}\n{}\n{}\n{}\n{}\n{}\n",
        get_midgard_api_url(),
        std::env::var("SURREAL_DATABASE_URL").expect("DATABASE_URL must be set"),
        std::env::var("POSTGRES_DATABASE_URL").expect("DATABASE_URL must be set"),
        std::env::var("MONGODB_DATABASE_URL").expect("DATABASE_URL must be set"),
        std::env::var("ROCKSDB_DATABASE_URL").expect("DATABASE_URL must be set"),
        std::env::var("LEVELDB_DATABASE_URL").expect("DATABASE_URL must be set"),
    );

    connect_db().await.expect("Failed to connect to SurrealDB");

    initialize_pg_pool(&std::env::var("POSTGRES_DATABASE_URL").expect("POSTGRES_URL must be set"))
        .await
        .expect("Failed to connect to PostgreSQL");

    connect_mongodb(&std::env::var("MONGODB_DATABASE_URL").expect("MONGODB_URL must be set"))
        .await
        .expect("Failed to connect to MongoDB");

    connect_rocksdb(&std::env::var("ROCKSDB_DATABASE_URL").expect("ROCKSDB_URL must be set"))
        .await
        .expect("Failed to connect to MongoDB");

    if let Err(e) =
        connect_leveldb(&std::env::var("LEVELDB_DATABASE_URL").expect("LEVELDB_URL must be set"))
            .await
    {
        tracing::error!("Failed to initialize LevelDB: {}", e);
    }

    if let Err(e) = fetch_and_store_initial_data().await {
        tracing::error!("Failed to fetch and store initial data: {}", e);
    }

    start_server().await;
}
