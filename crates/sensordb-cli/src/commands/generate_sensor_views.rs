use crate::cli::Connection;
use crate::error::Error;
use sensordb::DatabaseManager;
use std::time::Instant;
use tracing::info;

#[tokio::main]
pub async fn run(
    connection: &Connection,
    _campaign_name: &Option<String>,
    _mission_name: &Option<String>,
    _platform_name: &Option<String>,
    reflection_uncertainty_line_length: f32,
) -> Result<(), Error> {
    info!("Run generate sensor_views command in database",);

    let database_manager = DatabaseManager::new(
        &connection.get_connection_string(),
        connection.db_max_connections,
    )
    .await?;

    let start = Instant::now();
    database_manager
        .generate_sensor_views(reflection_uncertainty_line_length)
        .await?;

    let duration = std::time::Duration::from_secs(start.elapsed().as_secs());
    info!(
        "✅  Successful generation process took {} with {} connections.",
        humantime::format_duration(duration),
        connection.db_max_connections
    );

    Ok(())
}
