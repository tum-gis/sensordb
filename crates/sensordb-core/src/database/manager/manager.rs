use crate::Error;
use sqlx::postgres::PgPoolOptions;
use sqlx::{ConnectOptions, Pool, Postgres};
use tracing::log::LevelFilter;

const POOL_OVERHEAD_FACTOR: f32 = 1.2;

#[derive(Debug, Clone)]
pub struct DatabaseManager {
    pub(crate) pool: Pool<Postgres>,
    pub(crate) semaphore_permits: usize,
}

impl DatabaseManager {
    pub async fn new(database_url: &str, maximum_number_connections: u32) -> Result<Self, Error> {
        let options = database_url
            .parse::<sqlx::postgres::PgConnectOptions>()?
            .log_slow_statements(LevelFilter::Off, std::time::Duration::from_secs(30));

        let pool_connections = maximum_number_connections + (maximum_number_connections / 5);
        let pool = PgPoolOptions::new()
            .max_connections(pool_connections)
            .acquire_slow_level(LevelFilter::Off)
            .connect_with(options)
            .await?;

        Ok(DatabaseManager {
            pool,
            semaphore_permits: maximum_number_connections as usize,
        })
    }
}
