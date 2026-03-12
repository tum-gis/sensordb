use crate::cli::{CleanSubCommand, Connection};
use crate::error::Error;
use sensordb::DatabaseManager;
use std::time::Instant;
use tracing::info;

#[tokio::main]
pub async fn run(connection: &Connection, subcommand: &CleanSubCommand) -> Result<(), Error> {
    let database_manager = DatabaseManager::new(
        &connection.get_connection_string(),
        connection.db_max_connections,
    )
    .await?;
    info!(
        "Clean database at {} with {} connections",
        &connection.get_connection_string(),
        connection.db_max_connections
    );

    let start = Instant::now();
    match subcommand {
        CleanSubCommand::All => {
            database_manager.clean_all().await?;
        }
        CleanSubCommand::SensorViews => {
            database_manager.clean_sensor_views().await?;
        }
        CleanSubCommand::Associations => {
            database_manager.clean_associations().await?;
        }
    }

    let duration = std::time::Duration::from_secs(start.elapsed().as_secs());
    info!(
        "Cleaning process took {} with {} connections.",
        humantime::format_duration(duration),
        connection.db_max_connections
    );

    Ok(())
}
