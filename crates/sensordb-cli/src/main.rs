mod cli;
mod commands;
mod error;
mod util;

use crate::cli::{Cli, Command};
use anyhow::Result;
use clap::Parser;
use nalgebra::Point3;

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match &cli.command {
        Command::Info {} => {
            commands::info::run(&cli.connection)?;
        }
        Command::ImportRosbag {
            rosbag_directory_path,
            ecoord_file_path,
            start_date_time,
            end_date_time,
            start_time_offset,
            total_duration,
            global_frame_id,
            platform_frame_id,
            slice_duration,
            max_points_per_octant,
            campaign_name,
            mission_name,
            platform_name,
            metadata_only,
        } => {
            commands::import_rosbag::run(
                &cli.connection,
                rosbag_directory_path.canonicalize()?,
                ecoord_file_path,
                *start_date_time,
                *end_date_time,
                *start_time_offset,
                *total_duration,
                global_frame_id,
                platform_frame_id,
                *slice_duration,
                *max_points_per_octant,
                campaign_name.clone(),
                mission_name.clone(),
                platform_name.clone(),
                *metadata_only,
            )?;
        }
        Command::ImportPointCloud {
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
        } => {
            commands::import_point_cloud::run(
                &cli.connection,
                point_cloud_directory_path.canonicalize()?,
                ecoord_directory_path,
                *max_points_per_octant,
                campaign_name,
                mission_name,
                platform_name,
                sensor_name,
                *metadata_only,
                global_frame_id,
                platform_frame_id,
                sensor_frame_id,
            )?;
        }
        Command::GenerateSensorViews {
            campaign_name,
            mission_name,
            platform_name,
            reflection_uncertainty_line_length,
        } => {
            commands::generate_sensor_views::run(
                &cli.connection,
                campaign_name,
                mission_name,
                platform_name,
                *reflection_uncertainty_line_length,
            )?;
        }
        Command::Associate {
            reflection_uncertainty_point_buffer,
            reflection_uncertainty_line_buffer,
            max_reflection_uncertainty_line_intersection_parameter,
            maximum_return_number,
            include_all_returns,
        } => {
            let maximum_return_number = if *include_all_returns {
                None
            } else {
                Some(*maximum_return_number)
            };

            commands::associate::run(
                &cli.connection,
                *reflection_uncertainty_point_buffer,
                *reflection_uncertainty_line_buffer,
                *max_reflection_uncertainty_line_intersection_parameter,
                maximum_return_number,
            )?;
        }
        Command::EstimateSignatures {
            spherical_range_bin_boundaries,
            surface_zenith_angle_bin_boundaries,
            surface_azimuth_angle_bin_boundaries,
        } => {
            let surface_zenith_angle_bin_boundaries: Vec<f64> = surface_zenith_angle_bin_boundaries
                .iter()
                .map(|x| x.to_radians())
                .collect();
            let surface_azimuth_angle_bin_boundaries: Vec<f64> =
                surface_azimuth_angle_bin_boundaries
                    .iter()
                    .map(|x| x.to_radians())
                    .collect();

            commands::estimate_signatures::run(
                &cli.connection,
                spherical_range_bin_boundaries.clone(),
                surface_zenith_angle_bin_boundaries,
                surface_azimuth_angle_bin_boundaries,
            )?;
        }
        Command::Export {
            directory_path,
            point_cloud_format,
        } => {
            commands::export::run(
                &cli.connection,
                directory_path,
                point_cloud_format.to_epoint_format(),
            )?;
        }
        Command::CropCityModel {
            corner_min,
            corner_max,
        } => {
            let corner_min: Option<Point3<f64>> =
                corner_min.as_ref().map(|v| Point3::new(v[0], v[1], v[2]));
            let corner_max: Option<Point3<f64>> =
                corner_max.as_ref().map(|v| Point3::new(v[0], v[1], v[2]));

            commands::crop_city_model::run(&cli.connection, corner_min, corner_max)?;
        }
        Command::EnrichCityModel { subcommand } => {
            commands::enrich_city_model::run(&cli.connection, subcommand)?;
        }
        Command::ExportFeatureList {
            output_list_path,
            corner_min,
            corner_max,
        } => {
            let corner_min: Option<Point3<f64>> =
                corner_min.as_ref().map(|v| Point3::new(v[0], v[1], v[2]));
            let corner_max: Option<Point3<f64>> =
                corner_max.as_ref().map(|v| Point3::new(v[0], v[1], v[2]));

            commands::export_feature_list::run(
                &cli.connection,
                output_list_path,
                corner_min,
                corner_max,
            )?;
        }
        Command::ExportSignatures { output_path } => {
            commands::export_signatures::run(&cli.connection, output_path)?;
        }
        Command::Statistics {
            output_statistics_path,
            spherical_range_bin_size,
        } => {
            commands::statistics::run(
                &cli.connection,
                output_statistics_path,
                *spherical_range_bin_size,
            )?;
        }
        Command::Clean { subcommand } => {
            commands::clean::run(&cli.connection, subcommand)?;
        }
    };

    Ok(())
}
