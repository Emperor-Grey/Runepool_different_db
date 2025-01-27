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
    sync::{Arc, Mutex},
    time::Duration,
};
use surrealdb::{
    engine::remote::ws::{Client, Wss},
    opt::auth::Root,
    Result, Surreal,
};
use tracing::{error, info};

pub static DB: Lazy<Surreal<Client>> = Lazy::new(Surreal::init);
pub static PG_POOL: OnceCell<PgPool> = OnceCell::new();
pub static ROCKS_DB: OnceCell<Arc<rocksdb::DB>> = OnceCell::new();
pub static LEVEL_DB: OnceCell<Arc<Mutex<rusty_leveldb::DB>>> = OnceCell::new();
pub static MONGO_CLIENT: OnceCell<mongodb::Client> = OnceCell::new();

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
    let mut client_options = ClientOptions::parse(url).await?;

    let server_api = ServerApi::builder().version(ServerApiVersion::V1).build();
    client_options.server_api = Some(server_api);

    // Create the client
    let client = MongoClient::with_options(client_options)?;

    // Test the connection
    client
        .database("admin")
        .run_command(doc! {"ping": 1})
        .await?;

    // Store the client in the static MONGO_CLIENT
    if let Err(_) = MONGO_CLIENT.set(client) {
        error!("Failed to set MongoDB client");
        return Err(mongodb::error::Error::from(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to initialize MongoDB client",
        )));
    }

    info!("Connected to MongoDB!");
    Ok(())
}

pub async fn connect_rocksdb(url: &str) {
    let mut options = rocksdb::Options::default();
    options.create_if_missing(true);

    match rocksdb::DB::open(&options, url) {
        Ok(_db) => {
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
        Ok(_db) => {
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
        .min_connections(1)
        .max_lifetime(Some(Duration::from_secs(30 * 60))) // 30 minutes
        .idle_timeout(Some(Duration::from_secs(10 * 60))) // 10 minutes
        .connect(url)
        .await?;

    tracing::info!("Connected to PostgreSQL...");
    sqlx::migrate!("./migrations").run(&pool).await?;
    
    // Test the connection with a simple query
    sqlx::query("SELECT 1").fetch_one(&pool).await?;

    match PG_POOL.set(pool.clone()) {
        Ok(_) => Ok(pool),
        Err(_) => Err(sqlx::Error::Configuration(
            "Failed to initialize PG_POOL".into(),
        )),
    }
}
