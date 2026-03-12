use crate::cli::Connection;
use crate::error::Error;
use nalgebra::Point3;
use sensordb::DatabaseManager;
use std::fs::create_dir_all;
use std::path::Path;
use tracing::info;

#[tokio::main]
pub async fn run(
    connection: &Connection,
    output_list_path: impl AsRef<Path>,
    corner_min: Option<Point3<f64>>,
    corner_max: Option<Point3<f64>>,
) -> Result<(), Error> {
    info!("Export feature list");

    let database_manager = DatabaseManager::new(
        &connection.get_connection_string(),
        connection.db_max_connections,
    )
    .await?;

    create_dir_all(output_list_path.as_ref().parent().unwrap())
        .expect("Could not create output directory");

    database_manager
        .export_feature_list(output_list_path, corner_min, corner_max)
        .await?;

    Ok(())
}
