# DB Tester

A Rust-based application designed to benchmark and compare the performance of different databases when storing and querying blockchain data.

## Features

- **Database Support**:
  - SurrealDB
  - PostgreSQL
  - MongoDB
  - RocksDB
  - LevelDB
- **Blockchain Data**: Fetches blockchain data from the Midgard API.
- **REST API**: Provides an interface for querying stored data.
- **Performance Metrics**: Includes utilities for performance comparison.

## Setup

To get started, follow these steps:

1. Clone the repository:

   ```bash
   git clone https://github.com/Emperor-Grey/Runepool_different_db.git
   cd Runepool_different_db
   ```

2. Create a `.env` file in the root directory with the following environment variables for your database connection strings:

   ```env
   SURREAL_DATABASE_URL=your_surreal_db_connection_url
   POSTGRES_DATABASE_URL=your_postgres_db_connection_url
   MONGODB_DATABASE_URL=your_mongodb_connection_url
   ROCKSDB_DATABASE_URL=your_rocksdb_connection_url
   LEVELDB_DATABASE_URL=your_leveldb_connection_url
   MIDGARD_API_URL=your_midgard_api_url
   ```

3. Build the project using Cargo:

   ```bash
   cargo build
   ```

## Usage

Once you've set up your environment, run the application:

```bash
cargo run
```

This will:
- Connect to all the configured databases.
- Fetch the initial data from the Midgard API.
- Start the REST server to allow querying of stored data.

## API Endpoints

- `GET /runepools/:path`: Query the stored runepool data.
- `GET /runepools/surrealdb`: Query a specific runepool data stored in surrealdb.
- `GET /runepools/postgres`: Query a specific runepool data stored in postgres.
- `GET /runepools/mongodb`: Query a specific runepool data stored in mongodb.
- `GET /runepools/rocksdb`: Query a specific runepool data stored in rocksdb.
- `GET /runepools/leveldb`: Query a specific runepool data stored in leveldb.

## License

This project is licensed under the [MIT License](LICENSE).
