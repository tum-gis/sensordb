use crate::cli::Connection;

use crate::error::Error;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use ecoord::FrameId;
use erosbag::Rosbag;
use sensordb::DatabaseManager;
use std::path::Path;
use std::time::Instant;
use tracing::{info, warn};

#[tokio::main]
pub async fn run(
    connection: &Connection,
    rosbag_directory_path: impl AsRef<Path>,
    ecoord_file_path: impl AsRef<Path>,
    start_date_time: Option<DateTime<Utc>>,
    end_date_time: Option<DateTime<Utc>>,
    start_time_offset: Option<Duration>,
    total_duration: Option<Duration>,
    global_frame_id: &FrameId,
    platform_frame_id: &FrameId,
    slice_duration: chrono::Duration,
    max_points_per_octant: usize,
    campaign_name: String,
    mission_name: Option<String>,
    platform_name: String,
    metadata_only: bool,
) -> Result<(), Error> {
    let rosbag = Rosbag::new(rosbag_directory_path.as_ref())?;

    let transform_tree = ecoord::io::EcoordReader::from_path(ecoord_file_path)?.finish()?;

    let rosbag_start_date_time = match rosbag.get_start_date_time() {
        Ok(Some(date_time)) => date_time,
        Ok(None) => {
            panic!("Not able to retrieve start date time from Rosbag.")
        }
        Err(error) => {
            panic!("Problem opening the file: {error:?}");
        }
    };
    let rosbag_end_date_time = match rosbag.get_end_date_time() {
        Ok(Some(date_time)) => date_time,
        Ok(None) => {
            panic!("Not able to retrieve end date time from Rosbag.")
        }
        Err(error) => {
            panic!("Problem opening the file: {error:?}");
        }
    };
    info!(
        "Rosbag times: {rosbag_start_date_time} - {rosbag_end_date_time} with a duration of {}",
        humantime::format_duration((rosbag_end_date_time - rosbag_start_date_time).to_std()?)
    );

    let start_date_time: DateTime<Utc> =
        start_date_time.unwrap_or(rosbag_start_date_time) + start_time_offset.unwrap_or_default();
    let end_date_time: DateTime<Utc> = match (total_duration, end_date_time) {
        (Some(_total_duration), Some(end_date_time)) => {
            warn!("Both end_date_time and total_duration defined. Using end_date_time");
            end_date_time
        }
        (Some(total_duration), None) => start_date_time + total_duration,
        (None, Some(end_date_time)) => end_date_time,
        _ => rosbag_end_date_time,
    };

    let start_date_time = if rosbag_start_date_time <= start_date_time {
        start_date_time
    } else {
        warn!(
            "Defined start_date_time ({}) is before rosbag's start date time ({})",
            start_date_time, rosbag_start_date_time
        );
        rosbag_start_date_time
    };
    let end_date_time = if end_date_time <= rosbag_end_date_time {
        end_date_time
    } else {
        warn!(
            "Defined end_date_time ({}) is after rosbag's end date time ({})",
            end_date_time, rosbag_end_date_time
        );
        rosbag_end_date_time
    };

    info!(
        "Start importing to database with {} connections: {}",
        connection.db_max_connections,
        &connection.get_connection_string()
    );
    let database_manager = DatabaseManager::new(
        &connection.get_connection_string(),
        connection.db_max_connections,
    )
    .await?;
    let mission_name: String = if let Some(name) = mission_name {
        name.to_string()
    } else {
        rosbag_directory_path
            .as_ref()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into()
    };

    info!(
        "Import times: {start_date_time} - {end_date_time} with a duration of {}",
        humantime::format_duration((end_date_time - start_date_time).to_std()?)
    );
    let start = Instant::now();
    database_manager
        .import_rosbag(
            rosbag,
            transform_tree,
            Some(start_date_time),
            Some(end_date_time),
            global_frame_id,
            platform_frame_id,
            slice_duration,
            max_points_per_octant,
            campaign_name,
            mission_name,
            platform_name,
            metadata_only,
        )
        .await?;

    let duration = std::time::Duration::from_secs(start.elapsed().as_secs());
    info!(
        "✅  Successful import process took {} with {} connections.",
        humantime::format_duration(duration),
        connection.db_max_connections
    );

    Ok(())
}
