use crate::cli::Connection;
use crate::error::Error;
use sensordb::DatabaseManager;
use std::time::Instant;
use tracing::info;

#[tokio::main]
pub async fn run(
    connection: &Connection,
    spherical_range_bin_boundaries: Vec<f64>,
    surface_zenith_angle_bin_boundaries: Vec<f64>,
    surface_azimuth_angle_bin_boundaries: Vec<f64>,
) -> Result<(), Error> {
    info!("Estimate signatures");

    let start = Instant::now();
    let database_manager = DatabaseManager::new(
        &connection.get_connection_string(),
        connection.db_max_connections,
    )
    .await?;

    database_manager
        .estimate_signatures(
            spherical_range_bin_boundaries,
            surface_zenith_angle_bin_boundaries,
            surface_azimuth_angle_bin_boundaries,
        )
        .await?;

    let duration = std::time::Duration::from_secs(start.elapsed().as_secs());
    info!(
        "✅  Successful city model enrichment took {} with {} connections.",
        humantime::format_duration(duration),
        connection.db_max_connections
    );

    Ok(())
}
