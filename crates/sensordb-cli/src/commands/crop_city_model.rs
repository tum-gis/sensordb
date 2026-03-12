use crate::cli::Connection;
use crate::error::Error;
use nalgebra::Point3;
use sensordb::DatabaseManager;
use tracing::info;

#[tokio::main]
pub async fn run(
    connection: &Connection,
    corner_min: Option<Point3<f64>>,
    corner_max: Option<Point3<f64>>,
) -> Result<(), Error> {
    info!("Crop city model");

    if corner_min.is_none() || corner_max.is_none() {
        panic!("Please provide either corner_min and/or corner_max");
    }

    let database_manager = DatabaseManager::new(
        &connection.get_connection_string(),
        connection.db_max_connections,
    )
    .await?;

    database_manager
        .crop_city_model(corner_min, corner_max)
        .await?;

    Ok(())
}
