use crate::util::parse_duration;
use crate::util::parse_timestamp;
use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum, ValueHint};
use ecoord::FrameId;
use std::path::PathBuf;

#[derive(Parser)]
#[clap(author, version, about, long_about = None, propagate_version = true)]
pub struct Cli {
    #[command(flatten)]
    pub connection: Connection,

    #[clap(subcommand)]
    pub command: Command,
}

#[derive(Args)]
pub struct Connection {
    /// Database host address
    #[arg(long, env = "SENSORDB_HOST", default_value = "localhost", value_hint = ValueHint::Hostname)]
    db_host: String,

    /// Database port number
    #[arg(long, env = "SENSORDB_PORT", default_value_t = 5432)]
    db_port: u32,

    /// Database name
    #[arg(long, env = "SENSORDB_NAME", default_value = "sensordb")]
    pub db_name: String,

    /// Database username
    #[arg(long, env = "SENSORDB_USERNAME", default_value = "postgres")]
    pub db_username: String,

    /// Database password (consider using environment variable)
    #[arg(long, env = "SENSORDB_PASSWORD", hide_env_values = true)]
    pub db_password: String,

    /// Maximum number of connections to the database
    #[clap(long, env = "SENSORDB_MAX_CONNECTIONS", default_value_t = 10)]
    pub db_max_connections: u32,
}

impl Connection {
    pub fn get_connection_string(&self) -> String {
        format!(
            "postgresql://{}:{}@{}:{}/{}",
            self.db_username, self.db_password, self.db_host, self.db_port, self.db_name
        )
    }
}

#[derive(Subcommand)]
pub enum Command {
    /// Info
    Info {},

    /// Import ROS bag to the database
    ImportRosbag {
        /// Path to the rosbag to be imported
        #[clap(short, long, value_hint = clap::ValueHint::DirPath)]
        rosbag_directory_path: PathBuf,

        /// Path to additional georeferencing
        #[clap(long, value_hint = ValueHint::FilePath)]
        ecoord_file_path: PathBuf,

        /// The start time of the import in UTC.
        /// Example: 2020-04-12 22:10:57.123456789 +00:00
        /// If not provided, the import starts from the beginning.
        #[clap(long, value_parser = parse_timestamp)]
        start_date_time: Option<DateTime<Utc>>,

        /// The end time of the import in UTC.
        /// Example: 2020-04-12 22:10:57.123456789 +00:00
        /// If not provided, the import runs until the end of the available data.
        #[clap(long, value_parser = parse_timestamp)]
        end_date_time: Option<DateTime<Utc>>,

        /// The time offset applied to the rosbag import.
        /// Example: "5s" (5 seconds), "2m" (2 minutes).
        /// If not provided, no offset is applied.
        #[clap(long, value_parser = parse_duration)]
        start_time_offset: Option<chrono::Duration>,

        /// The total duration of the rosbag import.
        /// Example: "30s" (30 seconds), "1h" (1 hour).
        /// If not provided, the import runs until the end time or the end of the data.
        #[clap(long, value_parser = parse_duration)]
        total_duration: Option<chrono::Duration>,

        /// ID of the global coordinate frame for transforming the extracted point cloud.
        /// This frame serves as the target reference frame for all point cloud transformations.
        #[clap(long, default_value_t = FrameId::global())]
        global_frame_id: FrameId,

        #[clap(long, default_value_t = FrameId::base_link())]
        platform_frame_id: FrameId,

        /// The slice duration of the point cloud used for accumulating the data.
        /// Example: "30s" (30 seconds), "1h" (1 hour).
        #[clap(long, value_parser = parse_duration, default_value = "5s")]
        slice_duration: chrono::Duration,

        /// Maximum number of points per octant of octree
        #[clap(long, default_value_t = 100000)]
        max_points_per_octant: usize,

        /// Name of the campaign
        #[clap(long, default_value = "unnamed-campaign")]
        campaign_name: String,

        /// Name of the mission (if not set, name of the MCAP folder)
        #[clap(long)]
        mission_name: Option<String>,

        /// Name of the sensor platform
        #[clap(long, default_value = "unnamed-platform")]
        platform_name: String,

        /// Import only metadata without individual point geometries and point-level attributes
        #[clap(long, default_value_t = false)]
        metadata_only: bool,
    },

    /// Import point cloud to the database
    ImportPointCloud {
        /// Path to the directory containing the point clouds to be imported
        #[clap(short, long, value_hint = ValueHint::DirPath)]
        point_cloud_directory_path: PathBuf,

        /// Path to the directory containing ecoord files.
        /// Files are matched by name with corresponding point cloud files and incorporated if found.
        #[clap(long, value_hint = ValueHint::DirPath)]
        ecoord_directory_path: Option<PathBuf>,

        /// Maximum number of points per octant of octree
        #[clap(long, default_value_t = 100000)]
        max_points_per_octant: usize,

        /// Name of the campaign
        #[clap(long, default_value = "unnamed-campaign")]
        campaign_name: String,

        /// Name of the mission
        #[clap(long, default_value = "unnamed-mission")]
        mission_name: String,

        /// Name of the platform
        #[clap(long, default_value = "unnamed-platform")]
        platform_name: String,

        /// Name of the sensor
        #[clap(long, default_value = "unnamed-sensor")]
        sensor_name: String,

        /// Import only metadata without individual point geometries and point-level attributes
        #[clap(long, default_value_t = false)]
        metadata_only: bool,

        /// ID of the global coordinate frame for transforming the extracted point cloud.
        #[clap(long, default_value_t = FrameId::global())]
        global_frame_id: FrameId,

        #[clap(long, default_value_t = FrameId::platform())]
        platform_frame_id: FrameId,

        #[clap(long, default_value_t = FrameId::sensor())]
        sensor_frame_id: FrameId,
    },

    /// Computes the LiDAR beams and camera viewing frustums using the pose trajectory and sensor configuration
    GenerateSensorViews {
        /// Name of the campaign to process (processes all campaigns if not specified)
        #[clap(long)]
        campaign_name: Option<String>,

        /// Name of the mission to process (processes all missions if not specified)
        #[clap(long)]
        mission_name: Option<String>,

        /// Name of the platform to process (processes all platforms if not specified)
        #[clap(long)]
        platform_name: Option<String>,

        /// Length of the uncertainty line in the beam direction
        #[clap(long, default_value_t = 1.0)]
        reflection_uncertainty_line_length: f32,
    },

    /// Associate sensor data with model
    Associate {
        /// Buffer distance around the reflection point
        #[clap(long, default_value_t = 0.5)]
        reflection_uncertainty_point_buffer: f32,

        /// Buffer distance around the uncertainty line
        #[clap(long, default_value_t = 0.05)]
        reflection_uncertainty_line_buffer: f32,

        /// Length of the uncertainty line in the beam direction
        #[clap(long, default_value_t = 1.0)]
        max_reflection_uncertainty_line_intersection_parameter: f32,

        /// Maximum number
        #[clap(long, default_value_t = 1)]
        maximum_return_number: i32,

        #[clap(long, default_value_t = false)]
        include_all_returns: bool,
    },

    /// Estimate signatures
    EstimateSignatures {
        /// Bin boundaries of the spherical range in meter
        #[clap(
            long,
            default_value = "0.0,15.0,30.0,50.0,100.0,200.0,500.0,1000.0",
            value_delimiter = ','
        )]
        spherical_range_bin_boundaries: Vec<f64>,

        /// Bin boundaries of the surface zenith angle in degree
        #[clap(long, default_value = "0.0,20.0,40.0,60.0,90.0", value_delimiter = ',')]
        surface_zenith_angle_bin_boundaries: Vec<f64>,

        /// Bin boundaries of the surface azimuth angle in degree
        #[clap(long, default_value = "0.0,360.0", value_delimiter = ',')]
        surface_azimuth_angle_bin_boundaries: Vec<f64>,
    },

    /// Export sensor data from the database
    Export {
        /// Output directory path
        #[clap(short, long, value_hint = ValueHint::DirPath)]
        directory_path: PathBuf,

        /// Format in which to export the processed point clouds
        #[clap(long, default_value_t = PointCloudFormat::Epoint, value_enum)]
        point_cloud_format: PointCloudFormat,
    },

    /// Crop city model
    CropCityModel {
        /// Minimum corner coordinates of the bounding box for filtering features (x, y, z)
        #[clap(long, number_of_values = 3, allow_hyphen_values = true)]
        corner_min: Option<Vec<f64>>,

        /// Maximum corner coordinates of the bounding box for filtering features (x, y, z)
        #[clap(long, number_of_values = 3, allow_hyphen_values = true)]
        corner_max: Option<Vec<f64>>,
    },

    /// Enrich city model
    EnrichCityModel {
        #[clap(subcommand)]
        subcommand: EnrichCityModelSubCommand,
    },

    /// Export feature list
    ExportFeatureList {
        /// Path to the CSV file for the output feature list
        #[clap(long, value_hint = ValueHint::FilePath)]
        output_list_path: PathBuf,

        /// Minimum corner coordinates of the bounding box for filtering features (x, y, z)
        #[clap(long, number_of_values = 3, allow_hyphen_values = true)]
        corner_min: Option<Vec<f64>>,

        /// Maximum corner coordinates of the bounding box for filtering features (x, y, z)
        #[clap(long, number_of_values = 3, allow_hyphen_values = true)]
        corner_max: Option<Vec<f64>>,
    },

    /// Export signatures
    ExportSignatures {
        /// Path to the CSV file for the output feature list
        #[clap(long, value_hint = ValueHint::FilePath)]
        output_path: PathBuf,
    },

    /// Derive statistics
    Statistics {
        /// Path to the JSON file for the output statistics
        #[clap(long, value_hint = ValueHint::FilePath)]
        output_statistics_path: PathBuf,

        /// Bin size of the spherical range in meter
        #[clap(long, default_value_t = 15.0)]
        spherical_range_bin_size: f64,
    },

    /// Clean database from sensor data
    Clean {
        #[clap(subcommand)]
        subcommand: CleanSubCommand,
    },
}

#[derive(Subcommand, Debug, Copy, Clone, PartialEq, Eq)]
pub enum EnrichCityModelSubCommand {
    /// Enrich the city model with signatures as generic attributes
    Signature,
}

#[derive(Subcommand, Debug, Copy, Clone, PartialEq, Eq)]
pub enum CleanSubCommand {
    /// Remove all sensor data including sensor views and associations
    All,

    /// Remove only sensor views (LiDAR beams and camera viewing frustums)
    SensorViews,

    /// Remove only associations between sensor data and model features
    Associations,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, ValueEnum)]
pub enum PointCloudFormat {
    Epoint,
    EpointTar,
    E57,
    Las,
    Laz,
    Xyz,
    XyzZst,
}

impl PointCloudFormat {
    pub fn to_epoint_format(&self) -> epoint::io::PointCloudFormat {
        match self {
            Self::Epoint => epoint::io::PointCloudFormat::Epoint,
            Self::EpointTar => epoint::io::PointCloudFormat::EpointTar,
            Self::E57 => epoint::io::PointCloudFormat::E57,
            Self::Las => epoint::io::PointCloudFormat::Las,
            Self::Laz => epoint::io::PointCloudFormat::Laz,
            Self::Xyz => epoint::io::PointCloudFormat::Xyz,
            Self::XyzZst => epoint::io::PointCloudFormat::XyzZst,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, ValueEnum)]
pub enum PointCloudStatistic {
    PointsCount,
    IntensityMin,
    IntensityMax,
    IntensityMean,
    IntensityStandardDeviation,
    IntensityQ1,
    IntensityMedian,
    IntensityQ3,
    ReflectionPointSurfaceDistanceMean,
    ReflectionPointSurfaceDistanceStandardDeviation,
    ReflectionPointSurfaceDistanceQ1,
    ReflectionPointSurfaceDistanceMedian,
    ReflectionPointSurfaceDistanceQ3,
    TimestampSecMin,
    TimestampSecMax,
}
