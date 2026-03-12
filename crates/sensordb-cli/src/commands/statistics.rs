use crate::cli::Connection;
use crate::error::Error;
use sensordb::DatabaseManager;
use std::fs;
use std::path::Path;
use tracing::info;

#[tokio::main]
pub async fn run(
    connection: &Connection,
    output_statistics_path: impl AsRef<Path>,
    spherical_range_bin_size: f64,
) -> Result<(), Error> {
    info!("Run stats");

    let database_manager = DatabaseManager::new(
        &connection.get_connection_string(),
        connection.db_max_connections,
    )
    .await
    .unwrap();

    fs::create_dir_all(output_statistics_path.as_ref().parent().unwrap()).unwrap();
    database_manager
        .write_statistics(output_statistics_path, spherical_range_bin_size)
        .await?;

    Ok(())
}
