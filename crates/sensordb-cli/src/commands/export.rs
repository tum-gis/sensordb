use std::fs;

use crate::cli::Connection;
use crate::error::Error;
use epoint::io::PointCloudFormat;
use sensordb::DatabaseManager;
use std::path::Path;
use std::time::Instant;
use tracing::info;

#[tokio::main]
pub async fn run(
    connection: &Connection,
    directory_path: impl AsRef<Path>,
    point_cloud_format: PointCloudFormat,
) -> Result<(), Error> {
    info!(
        "Start export to directory: {}",
        directory_path.as_ref().display()
    );

    if directory_path.as_ref().exists() {
        fs::remove_dir_all(&directory_path).expect("TODO: panic message");
    }
    fs::create_dir_all(&directory_path).unwrap();

    let database_manager = DatabaseManager::new(
        &connection.get_connection_string(),
        connection.db_max_connections,
    )
    .await?;

    let start = Instant::now();

    info!("[1/2] ⬇️🔦 Export sensor infos");
    database_manager.export_sensor_info(&directory_path).await?;

    database_manager
        .export(&directory_path, point_cloud_format)
        .await?;

    /*match subcommand {
        ExportPointCloudSubCommand::Packages => {
            database_manager
                .export_all_point_cloud_packages(&directory_path, point_cloud_format)
                .await?;
        }
        ExportPointCloudSubCommand::Recordings => {
            database_manager
                .export_all_point_cloud_time_per_recording(&directory_path, point_cloud_format)
                .await?;
        }
        ExportPointCloudSubCommand::TimeSlices { step_duration } => {
            database_manager
                .export_all_point_cloud_time_slices(
                    &directory_path,
                    point_cloud_format,
                    step_duration,
                )
                .await?;
        }
        ExportPointCloudSubCommand::AssociatedFeatures => {
            database_manager
                .export_all_point_cloud_associated_features(&directory_path, point_cloud_format)
                .await?;
        }
    }*/
    let duration = std::time::Duration::from_secs(start.elapsed().as_secs());
    info!(
        "✅  Successful export process took {} with {} connections.",
        humantime::format_duration(duration),
        connection.db_max_connections
    );

    Ok(())
}
