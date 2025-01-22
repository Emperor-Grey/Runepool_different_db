use mongodb::{
    bson::doc,
    options::{ClientOptions, ServerApi, ServerApiVersion},
    Client as MongoClient,
};
use once_cell::sync::Lazy;
use once_cell::sync::OnceCell;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::{
    env,
    path::{self, Path},
    sync::{Arc, Mutex},
    time::Duration,
};
use surrealdb::{
    engine::remote::ws::{Client, Wss},
    opt::auth::{Jwt, Root},
    Result, Surreal,
};
use tracing::{error, info};

pub static DB: Lazy<Surreal<Client>> = Lazy::new(Surreal::init);
pub static PG_POOL: OnceCell<PgPool> = OnceCell::new();
pub static ROCKS_DB: OnceCell<Arc<rocksdb::DB>> = OnceCell::new();
pub static LEVEL_DB: OnceCell<Arc<Mutex<rusty_leveldb::DB>>> = OnceCell::new();

pub async fn connect_db() -> Result<()> {
    let database_url = env::var("SURREAL_DATABASE_URL").expect("DATABASE_URL must be set");

    match DB.connect::<Wss>(&database_url).await {
        Ok(_) => info!("Connected to Surreal DB"),
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

pub async fn connect_mongodb(url: &str) -> mongodb::error::Result<()> {
    // Use the passed URL parameter instead of hardcoding it
    let mut client_options = ClientOptions::parse(url).await?;

    // Set the server_api field for the Stable API version
    let server_api = ServerApi::builder().version(ServerApiVersion::V1).build();
    client_options.server_api = Some(server_api);

    // Get a handle to the cluster
    let client = MongoClient::with_options(client_options)?;

    // Ping the server to see if you can connect to the cluster
    client
        .database("admin")
        .run_command(doc! {"ping": 1})
        .await?;

    info!("Connected to MongoDB!");
    Ok(())
}
pub async fn connect_rocksdb(url: &str) {
    let mut options = rocksdb::Options::default();
    options.create_if_missing(true);

    // Try opening the RocksDB database at the given URL
    match rocksdb::DB::open(&options, url) {
        Ok(db) => {
            info!("Successfully connected to RocksDB at {}", url);
            // Example: db.put(b"key", b"value").unwrap();
        }
        Err(e) => {
            error!("Failed to connect to RocksDB at {}: {}", url, e);
        }
    }
}

pub async fn connect_leveldb(url: &str) {
    let opt = rusty_leveldb::Options::default();
    match rusty_leveldb::DB::open(url, opt) {
        Ok(db) => {
            info!("Successfully connected to LevelDB at {:?}", url);
        }
        Err(e) => {
            error!("Failed to connect to LevelDB at {:?}: {}", url, e);
            panic!("Failed to connect to LevelDB");
        }
    }
}

pub async fn initialize_pg_pool(url: &str) -> sqlx::Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(3))
        .connect(url)
        .await
        .expect("Failed to connect to database");

    tracing::info!("Connected to PostgreSQL...");

    PG_POOL
        .set(pool.clone())
        .expect("PG_POOL already initialized");

    Ok(pool)
}
