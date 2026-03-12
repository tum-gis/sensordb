use crate::cli::Connection;
use crate::error::Error;
use sensordb::DatabaseManager;
use std::fs::create_dir_all;
use std::path::Path;
use tracing::info;

#[tokio::main]
pub async fn run(connection: &Connection, output_path: impl AsRef<Path>) -> Result<(), Error> {
    info!("Export signatures");

    let database_manager = DatabaseManager::new(
        &connection.get_connection_string(),
        connection.db_max_connections,
    )
    .await?;

    create_dir_all(output_path.as_ref().parent().unwrap())
        .expect("Could not create output directory");

    database_manager.export_signatures(output_path).await?;

    Ok(())
}
