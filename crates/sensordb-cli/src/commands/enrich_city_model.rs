use crate::cli::{Connection, EnrichCityModelSubCommand};
use crate::error::Error;
use sensordb::DatabaseManager;
use std::time::Instant;
use tracing::info;

#[tokio::main]
pub async fn run(
    connection: &Connection,
    subcommand: &EnrichCityModelSubCommand,
) -> Result<(), Error> {
    info!("Enrich city model");

    let start = Instant::now();
    let database_manager = DatabaseManager::new(
        &connection.get_connection_string(),
        connection.db_max_connections,
    )
    .await?;

    match subcommand {
        EnrichCityModelSubCommand::Signature => {
            database_manager
                .enrich_city_model_by_signatures()
                .await
                .unwrap();
        }
    }

    let duration = std::time::Duration::from_secs(start.elapsed().as_secs());
    info!(
        "✅  Successful city model enrichment took {} with {} connections.",
        humantime::format_duration(duration),
        connection.db_max_connections
    );

    Ok(())
}
