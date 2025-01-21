use std::{env, time::Duration};

use once_cell::sync::Lazy;
use once_cell::sync::OnceCell;
use sqlx::{postgres::PgPoolOptions, PgPool};
use surrealdb::{
    engine::remote::ws::{Client, Wss},
    opt::auth::{Jwt, Root},
    Result, Surreal,
};
use tracing::{error, info};

pub static DB: Lazy<Surreal<Client>> = Lazy::new(Surreal::init);
pub static PG_POOL: OnceCell<PgPool> = OnceCell::new();

pub async fn connect_db() -> Result<()> {
    let database_url = env::var("SURREAL_DATABASE_URL").expect("DATABASE_URL must be set");

    match DB.connect::<Wss>(&database_url).await {
        Ok(_) => info!("Connected to DB"),
        Err(e) => error!("Failed to connect to DB: {}", e),
    }

    match DB
        .signin(Root {
            username: "root",
            password: "root",
        })
        .await
    {
        Ok(_) => info!("Signed in with root credentials"),
        Err(e) => error!("Failed to sign in: {}", e),
    }

    let namespace = env::var("SURREAL_NAMESPACE").unwrap_or_else(|_| String::from("runepool"));
    let database = env::var("SURREAL_DATABASE").unwrap_or_else(|_| String::from("runepool"));

    match DB.use_ns(&namespace).use_db(&database).await {
        Ok(_) => info!("Using namespace and database"),
        Err(e) => error!("Failed to set namespace and database: {}", e),
    }

    Ok(())
}

pub async fn connect_postgres(url: &str) -> sqlx::Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(3))
        .connect(url)
        .await
        .expect("Failed to connect to database");

    tracing::info!("Connected to postgres");

    Ok(())
}

pub async fn connect_mongodb(url: &str) {}
pub async fn connect_rocksdb(url: &str) {}
pub async fn connect_leveldb(url: &str) {}

pub async fn initialize_pg_pool(url: &str) -> sqlx::Result<PgPool> {
    let pool = PgPoolOptions::new().max_connections(5).connect(url).await?;

    // Safe initialization
    PG_POOL
        .set(pool.clone())
        .expect("PG_POOL already initialized");

    Ok(pool)
}
