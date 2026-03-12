use serde::{Deserialize, Serialize};
use sqlx::Type;
use std::fmt;
use strum_macros::{AsRefStr, EnumString, FromRepr};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Type, FromRepr)]
#[repr(i32)]
pub enum DataType {
    Boolean = 1,
    Int4 = 2,
    Int8 = 3,
    Float4 = 4,
    Float8 = 5,
    String = 6,
    Timestamp = 7,
    Quaternion = 8,

    BooleanArray = 9,
    Int4Array = 10,
    Int8Array = 11,
    Float4Array = 12,
    Float8Array = 13,
    StringArray = 14,
    TimestampArray = 15,
    QuaternionArray = 16,

    GeometryPoint = 17,
    GeometryLineString = 18,
    GeometryPolygon = 19,
    GeometryMultiPoint = 20,
    GeometryMultiLineString = 21,

    GeometryReferenceArray = 22,
}

/*impl DataType {
    /// Returns the typename as stored in the database
    pub fn typename(&self) -> &'static str {
        match self {
            Self::Boolean => "Boolean",
            Self::Int4 => "Integer",
            Self::Float8 => "Float8",
            Self::String => "String",
            Self::Timestamp => "Timestamp",
            Self::Quaternion => "Quaternion",
            Self::BooleanArray => "BooleanArray",
            Self::IntegerArray => "IntegerArray",
            Self::DoubleArray => "DoubleArray",
            Self::StringArray => "StringArray",
            Self::TimestampArray => "TimestampArray",
            Self::QuaternionArray => "QuaternionArray",
            Self::GeometryPoint => "GeometryPoint",
            Self::GeometryLineString => "GeometryLineString",
            Self::GeometryPolygon => "GeometryPolygon",
            Self::GeometryMultiPoint => "GeometryMultiPoint",
            Self::GeometryMultiLineString => "GeometryMultiLineString",
            Self::GeometryReferenceArray => "GeometryReferenceArray",
        }
    }
}*/

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", *self as i32)
    }
}

/// Database datatype identifiers for sensordb.datatype table
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Type)]
#[repr(i32)]
pub enum Namespace {
    Core = 1,
    Generic = 2,
}

impl Namespace {
    /// Returns the typename as stored in the database
    pub fn typename(&self) -> &'static str {
        match self {
            Self::Core => "Core",
            Self::Generic => "Generic",
        }
    }
}

impl std::fmt::Display for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", *self as i32)
    }
}

// #[strum(serialize_all = "camelCase")]

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr, EnumString)]
pub enum PointCloudCellDataName {
    ReflectionPoint,
    Id,
    TimestampSecond,
    TimestampNanoSecond,
    Intensity,
    SensorPosition,
    SensorOrientation,
    BeamLine,
    BeamDirection,
    SphericalAzimuth,
    SphericalElevation,
    SphericalRange,
    ReflectionUncertaintyLine,
    ReflectionEnvelope,
    SensorPositionEnvelope,
    FeatureGeometryId,
    ReflectionPointSurfaceDistance,
    BeamLineSurfaceDistance,
    SurfaceZenithAngle,
    SurfaceAzimuthAngle,
    ReflectionLinePlaneIntersectionParameter,
    PointSourceId,
}

impl PointCloudCellDataName {
    pub fn datatype(&self) -> DataType {
        match self {
            PointCloudCellDataName::ReflectionPoint => DataType::GeometryMultiPoint,
            PointCloudCellDataName::Id => DataType::Int4Array,
            PointCloudCellDataName::TimestampSecond => DataType::Int4Array,
            PointCloudCellDataName::TimestampNanoSecond => DataType::Int4Array,
            PointCloudCellDataName::Intensity => DataType::Float8Array,
            PointCloudCellDataName::SensorPosition => DataType::GeometryMultiPoint,
            PointCloudCellDataName::SensorOrientation => DataType::QuaternionArray,
            PointCloudCellDataName::BeamLine => DataType::GeometryMultiLineString,
            PointCloudCellDataName::BeamDirection => DataType::GeometryMultiPoint,
            PointCloudCellDataName::SphericalAzimuth => DataType::Float8Array,
            PointCloudCellDataName::SphericalElevation => DataType::Float8Array,
            PointCloudCellDataName::SphericalRange => DataType::Float8Array,
            PointCloudCellDataName::ReflectionUncertaintyLine => DataType::GeometryMultiLineString,
            PointCloudCellDataName::ReflectionEnvelope => DataType::GeometryPolygon,
            PointCloudCellDataName::SensorPositionEnvelope => DataType::GeometryPolygon,
            PointCloudCellDataName::FeatureGeometryId => DataType::GeometryReferenceArray,
            PointCloudCellDataName::ReflectionPointSurfaceDistance => DataType::Float8Array,
            PointCloudCellDataName::BeamLineSurfaceDistance => DataType::Float8Array,
            PointCloudCellDataName::SurfaceZenithAngle => DataType::Float8Array,
            PointCloudCellDataName::SurfaceAzimuthAngle => DataType::Float8Array,
            PointCloudCellDataName::ReflectionLinePlaneIntersectionParameter => {
                DataType::Float8Array
            }
            PointCloudCellDataName::PointSourceId => DataType::Int4Array,
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Type, Serialize, Deserialize,
)]
#[sqlx(transparent, type_name = "INT4")]
pub struct DatatypeId(i32);

impl fmt::Display for DatatypeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i32> for DatatypeId {
    fn from(id: i32) -> Self {
        Self(id)
    }
}

impl From<DatatypeId> for i32 {
    fn from(id: DatatypeId) -> Self {
        id.0
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Type, Serialize, Deserialize,
)]
#[sqlx(transparent, type_name = "INT4")]
pub struct NamespaceId(i32);

impl fmt::Display for NamespaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i32> for NamespaceId {
    fn from(id: i32) -> Self {
        Self(id)
    }
}

impl From<NamespaceId> for i32 {
    fn from(id: NamespaceId) -> Self {
        id.0
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Type, Serialize, Deserialize,
)]
#[sqlx(transparent, type_name = "INT4")]
pub struct SensorId(i32);

impl fmt::Display for SensorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i32> for SensorId {
    fn from(id: i32) -> Self {
        Self(id)
    }
}

impl From<SensorId> for i32 {
    fn from(id: SensorId) -> Self {
        id.0
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Type, Serialize, Deserialize,
)]
#[sqlx(transparent, type_name = "INT4")]
pub struct PlatformId(i32);

impl fmt::Display for PlatformId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i32> for PlatformId {
    fn from(id: i32) -> Self {
        Self(id)
    }
}

impl From<PlatformId> for i32 {
    fn from(id: PlatformId) -> Self {
        id.0
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Type, Serialize, Deserialize,
)]
#[sqlx(transparent, type_name = "INT4")]
pub struct CampaignId(i32);

impl fmt::Display for CampaignId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i32> for CampaignId {
    fn from(id: i32) -> Self {
        Self(id)
    }
}

impl From<CampaignId> for i32 {
    fn from(id: CampaignId) -> Self {
        id.0
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Type, Serialize, Deserialize,
)]
#[sqlx(transparent, type_name = "INT4")]
pub struct MissionId(i32);

impl fmt::Display for MissionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i32> for MissionId {
    fn from(id: i32) -> Self {
        Self(id)
    }
}

impl From<MissionId> for i32 {
    fn from(id: MissionId) -> Self {
        id.0
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Type, Serialize, Deserialize,
)]
#[sqlx(transparent, type_name = "INT4")]
pub struct RecordingId(i32);

impl fmt::Display for RecordingId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i32> for RecordingId {
    fn from(id: i32) -> Self {
        Self(id)
    }
}

impl From<RecordingId> for i32 {
    fn from(id: RecordingId) -> Self {
        id.0
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Type, Serialize, Deserialize,
)]
#[sqlx(transparent, type_name = "INT4")]
pub struct TrajectoryId(i32);

impl fmt::Display for TrajectoryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i32> for TrajectoryId {
    fn from(id: i32) -> Self {
        Self(id)
    }
}

impl From<TrajectoryId> for i32 {
    fn from(id: TrajectoryId) -> Self {
        id.0
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Type, Serialize, Deserialize,
)]
#[sqlx(transparent, type_name = "INT4")]
pub struct PointCloudId(i32);

impl fmt::Display for PointCloudId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i32> for PointCloudId {
    fn from(id: i32) -> Self {
        Self(id)
    }
}

impl From<PointCloudId> for i32 {
    fn from(id: PointCloudId) -> Self {
        id.0
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Type, Serialize, Deserialize,
)]
#[sqlx(transparent, type_name = "INT4")]
pub struct PointCloudCellId(i32);

impl fmt::Display for PointCloudCellId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i32> for PointCloudCellId {
    fn from(id: i32) -> Self {
        Self(id)
    }
}

impl From<PointCloudCellId> for i32 {
    fn from(id: PointCloudCellId) -> Self {
        id.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Type, Serialize, Deserialize)]
#[sqlx(transparent, type_name = "TEXT")]
pub struct FeatureClassName(pub String);

impl fmt::Display for FeatureClassName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Type, Serialize, Deserialize)]
#[sqlx(transparent, type_name = "TEXT")]
pub struct FeatureObjectName(pub String);

impl fmt::Display for FeatureObjectName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Type, Serialize, Deserialize,
)]
#[sqlx(transparent, type_name = "INT8")]
pub struct FeatureId(pub i64);

impl fmt::Display for FeatureId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
