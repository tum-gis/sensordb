use crate::cli::Connection;
use crate::error::Error;
use ecoord::FrameId;
use sensordb::DatabaseManager;
use std::path::Path;
use std::time::{Duration, Instant};
use tracing::info;

#[tokio::main]
pub async fn run(
    connection: &Connection,
    point_cloud_directory_path: impl AsRef<Path>,
    ecoord_directory_path: &Option<impl AsRef<Path>>,
    max_points_per_octant: usize,
    campaign_name: &str,
    mission_name: &str,
    platform_name: &str,
    sensor_name: &str,
    metadata_only: bool,
    global_frame_id: &FrameId,
    platform_frame_id: &FrameId,
    sensor_frame_id: &FrameId,
) -> Result<(), Error> {
    info!(
        "Start importing to database with {} connections: {}",
        connection.db_max_connections,
        &connection.get_connection_string()
    );
    let start = Instant::now();

    let database_manager = DatabaseManager::new(
        &connection.get_connection_string(),
        connection.db_max_connections,
    )
    .await?;
    database_manager
        .import_point_cloud_directory(
            point_cloud_directory_path,
            ecoord_directory_path,
            max_points_per_octant,
            campaign_name,
            mission_name,
            platform_name,
            sensor_name,
            metadata_only,
            global_frame_id,
            platform_frame_id,
            sensor_frame_id,
        )
        .await?;

    let duration = Duration::from_secs(start.elapsed().as_secs());
    info!(
        "✅  Successful point cloud import took {} with {} connections.",
        humantime::format_duration(duration),
        connection.db_max_connections
    );

    Ok(())
}
