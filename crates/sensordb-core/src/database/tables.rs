use chrono::{DateTime, Timelike, Utc};
use ecoord::{TimedTransform, Transform};
use sqlx::FromRow;

use crate::database::datatype::{
    CampaignId, DatatypeId, MissionId, NamespaceId, PlatformId, PointCloudCellDataName,
    PointCloudCellId, PointCloudId, RecordingId, SensorId, TrajectoryId,
};
use serde::{Deserialize, Serialize};
use sqlx::Type;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type)]
#[sqlx(type_name = "text", rename_all = "lowercase")]
pub enum SensorType {
    Lidar,
    Camera,
    Radar,
}

#[derive(FromRow)]
pub struct SensorEntry {
    pub id: SensorId,
    pub platform_id: PlatformId,
    pub name: String,
    #[sqlx(rename = "type")]
    pub sensor_type: SensorType,
    pub manufacturer: Option<String>,
    pub model_number: Option<String>,
}

#[derive(FromRow)]
pub struct PlatformEntry {
    pub id: PlatformId,
    pub name: String,
    pub platform_type: Option<String>,
    pub description: Option<String>,
}

#[derive(FromRow)]
pub struct CampaignEntry {
    pub id: CampaignId,
    pub name: String,
}

#[derive(FromRow)]
pub struct MissionEntry {
    pub id: MissionId,
    pub name: Option<String>,
}

#[derive(FromRow)]
pub struct RecordingEntry {
    pub id: RecordingId,
    pub parent_id: Option<i32>,
    pub mission_id: MissionId,
    pub platform_id: Option<i32>,
    pub sensor_id: Option<i32>,
    pub name: Option<String>,
    pub start_date: Option<DateTime<Utc>>,
    pub end_date: Option<DateTime<Utc>>,
}

#[derive(sqlx::Type, Debug, Clone, Copy)]
#[sqlx(type_name = "sensordb.trajectory_domain", rename_all = "lowercase")]
pub enum TrajectoryDomain {
    Timed,
    Sequence,
}

#[derive(sqlx::Type, Debug, Clone, Copy)]
#[sqlx(type_name = "sensordb.interpolation_type", rename_all = "lowercase")]
pub enum InterpolationType {
    Step,
    Linear,
}

#[derive(sqlx::Type, Debug, Clone, Copy)]
#[sqlx(type_name = "sensordb.extrapolation_type", rename_all = "lowercase")]
pub enum ExtrapolationType {
    Constant,
}

#[derive(FromRow)]
pub struct TrajectoryEntry {
    pub id: TrajectoryId,
    pub recording_id: RecordingId,
    pub domain: TrajectoryDomain,
    pub interpolation_type: InterpolationType,
    pub extrapolation_type: ExtrapolationType,
}

#[derive(Debug, Clone)]
pub struct TrajectoryPoseEntry {
    pub trajectory_id: TrajectoryId,
    pub timestamp_sec: Option<i64>,
    pub timestamp_nanosec: Option<i32>,
    pub sequence_index: Option<i32>,
    pub position: Option<(f64, f64, f64)>, // (x, y, z)
    pub orientation: Option<QuaternionEntry>,
}

impl TrajectoryPoseEntry {
    pub fn from_timed_transform(
        trajectory_id: TrajectoryId,
        timed_transform: TimedTransform,
    ) -> Self {
        Self {
            trajectory_id,
            timestamp_sec: Some(timed_transform.timestamp.timestamp()),
            timestamp_nanosec: Some(timed_transform.timestamp.nanosecond() as i32),
            sequence_index: None,
            position: Some((
                timed_transform.transform.translation.x,
                timed_transform.transform.translation.y,
                timed_transform.transform.translation.z,
            )),
            orientation: Some(timed_transform.transform.rotation.into()),
        }
    }

    pub fn from_transform(
        trajectory_id: TrajectoryId,
        sequence_index: i32,
        transform: Transform,
    ) -> Self {
        Self {
            trajectory_id,
            timestamp_sec: None,
            timestamp_nanosec: None,
            sequence_index: Some(sequence_index),
            position: Some((
                transform.translation.x,
                transform.translation.y,
                transform.translation.z,
            )),
            orientation: Some(transform.rotation.into()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct QuaternionEntry {
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub w: f64,
}

impl From<nalgebra::UnitQuaternion<f64>> for QuaternionEntry {
    fn from(item: nalgebra::UnitQuaternion<f64>) -> Self {
        Self {
            x: item.i,
            y: item.j,
            z: item.k,
            w: item.w,
        }
    }
}

#[derive(FromRow)]
pub struct PointCloudEntry {
    pub id: PointCloudId,
    pub recording_id: Option<i32>,
    pub name: Option<String>,
    pub start_date: Option<DateTime<Utc>>,
    pub end_date: Option<DateTime<Utc>>,
}

#[derive(FromRow)]
pub struct PointCloudCellEntry {
    pub id: PointCloudCellId,
    pub point_cloud_id: PointCloudId,
    pub level: i32,
    pub x: i32,
    pub y: i32,
    pub z: i32,
}

#[derive(Debug, Clone)]
pub(crate) struct PointCloudAttributeContext {
    pub entries: Vec<PointCloudAttributeContextEntry>,
}

impl PointCloudAttributeContext {
    pub fn from(entries: Vec<PointCloudAttributeContextEntry>) -> Self {
        Self { entries }
    }

    pub fn contains_cell_data_name(&self, name: PointCloudCellDataName) -> bool {
        self.entries.iter().any(|x| x.name == name.as_ref())
    }
}

//
#[derive(Debug, Clone)]
pub(crate) struct PointCloudAttributeContextEntry {
    pub name: String,
    pub datatype_id: DatatypeId,
    pub namespace_id: NamespaceId,
    pub is_consistent: bool,
}
