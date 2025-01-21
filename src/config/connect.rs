use std::env;

use once_cell::sync::Lazy;
use surrealdb::{
    engine::remote::ws::{Client, Wss},
    opt::auth::{Jwt, Root},
    Result, Surreal,
};
use tracing::{error, info};

pub static DB: Lazy<Surreal<Client>> = Lazy::new(Surreal::init);

pub async fn connect_db() -> Result<()> {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

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
