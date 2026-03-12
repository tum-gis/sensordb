use polars::datatypes::PlSmallStr;

const COLUMN_NAME_MCAP_CHUNK_ID_STR: &str = "mcap_chunk_id";
const COLUMN_NAME_MCAP_MESSAGE_ID_STR: &str = "mcap_message_id";
const COLUMN_NAME_PACKAGE_ID_STR: &str = "package_id";
const COLUMN_NAME_POINT_INDEX_STR: &str = "point_index";
const COLUMN_NAME_SENSOR_ID_STR: &str = "sensor_id";
const COLUMN_NAME_CAMPAIGN_ID_STR: &str = "campaign_id";
const COLUMN_NAME_FEATURE_OBJECT_ID_STR: &str = "feature_object_id";
const COLUMN_NAME_FEATURE_OBJECT_NAME_STR: &str = "feature_object_name";
const COLUMN_NAME_FEATURE_CLASS_NAME_STR: &str = "feature_class_name";
const COLUMN_NAME_REFLECTION_POINT_SURFACE_DISTANCE_STR: &str = "reflection_point_surface_distance";
const COLUMN_NAME_BEAM_LINE_SURFACE_DISTANCE_STR: &str = "beam_line_surface_distance";
const COLUMN_NAME_REFLECTION_LINE_PLANE_INTERSECTION_PARAMETER_STR: &str =
    "reflection_line_plane_intersection_parameter";
const COLUMN_NAME_RETURN_NUMBER_PARAMETER_STR: &str = "return_number";
const COLUMN_NAME_SURFACE_ZENITH_ANGLE_STR: &str = "surface_zenith_angle";
const COLUMN_NAME_SURFACE_AZIMUTH_ANGLE_STR: &str = "surface_azimuth_angle";

/// Additional column names for ROS specific fields for `epoint::PointCloud`.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ExtendedPointDataColumnType {
    McapChunkId,
    McapMessageId,
    PackageId,
    PointIndex,
    SensorId,
    CampaignId,
    FeatureObjectId,
    FeatureObjectName,
    FeatureClassName,
    ReflectionPointSurfaceDistance,
    BeamLineSurfaceDistance,
    ReflectionLinePlaneIntersectionParameter,
    ReturnNumber,
    SurfaceZenithAngle,
    SurfaceAzimuthAngle,
}

impl ExtendedPointDataColumnType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExtendedPointDataColumnType::McapChunkId => COLUMN_NAME_MCAP_CHUNK_ID_STR,
            ExtendedPointDataColumnType::McapMessageId => COLUMN_NAME_MCAP_MESSAGE_ID_STR,
            ExtendedPointDataColumnType::PackageId => COLUMN_NAME_PACKAGE_ID_STR,
            ExtendedPointDataColumnType::PointIndex => COLUMN_NAME_POINT_INDEX_STR,
            ExtendedPointDataColumnType::SensorId => COLUMN_NAME_SENSOR_ID_STR,
            ExtendedPointDataColumnType::CampaignId => COLUMN_NAME_CAMPAIGN_ID_STR,
            ExtendedPointDataColumnType::FeatureObjectId => COLUMN_NAME_FEATURE_OBJECT_ID_STR,
            ExtendedPointDataColumnType::FeatureObjectName => COLUMN_NAME_FEATURE_OBJECT_NAME_STR,
            ExtendedPointDataColumnType::FeatureClassName => COLUMN_NAME_FEATURE_CLASS_NAME_STR,
            ExtendedPointDataColumnType::ReflectionPointSurfaceDistance => {
                COLUMN_NAME_REFLECTION_POINT_SURFACE_DISTANCE_STR
            }
            ExtendedPointDataColumnType::BeamLineSurfaceDistance => {
                COLUMN_NAME_BEAM_LINE_SURFACE_DISTANCE_STR
            }
            ExtendedPointDataColumnType::ReflectionLinePlaneIntersectionParameter => {
                COLUMN_NAME_REFLECTION_LINE_PLANE_INTERSECTION_PARAMETER_STR
            }
            ExtendedPointDataColumnType::ReturnNumber => COLUMN_NAME_RETURN_NUMBER_PARAMETER_STR,
            ExtendedPointDataColumnType::SurfaceZenithAngle => COLUMN_NAME_SURFACE_ZENITH_ANGLE_STR,
            ExtendedPointDataColumnType::SurfaceAzimuthAngle => {
                COLUMN_NAME_SURFACE_AZIMUTH_ANGLE_STR
            }
        }
    }
}

impl From<ExtendedPointDataColumnType> for PlSmallStr {
    fn from(value: ExtendedPointDataColumnType) -> Self {
        value.as_str().into()
    }
}
