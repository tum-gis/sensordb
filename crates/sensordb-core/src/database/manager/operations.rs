use crate::database::datatype::{CampaignId, MissionId, PlatformId, RecordingId, SensorId};
use crate::database::tables::{
    CampaignEntry, ExtrapolationType, InterpolationType, MissionEntry, PlatformEntry,
    PointCloudEntry, RecordingEntry, SensorEntry, SensorType, TrajectoryDomain, TrajectoryEntry,
};
use crate::{DatabaseManager, Error};
use chrono::{DateTime, Utc};

impl DatabaseManager {
    pub async fn register_or_get_platform(&self, name: String) -> Result<PlatformEntry, Error> {
        let platform = sqlx::query_as!(
            PlatformEntry,
            r#"
    INSERT INTO sensordb.platform (name, platform_type, description)
    VALUES ($1, NULL, NULL)
    ON CONFLICT (name) DO UPDATE SET name = platform.name
    RETURNING id, name, platform_type, description
    "#,
            name
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(platform)
    }

    pub(crate) async fn register_or_get_sensor(
        &self,
        sensor_name: String,
        sensor_type: SensorType,
        platform_id: PlatformId,
    ) -> Result<SensorEntry, Error> {
        let platform_id: i32 = platform_id.into();

        let sensor = sqlx::query_as!(
            SensorEntry,
            r#"
    INSERT INTO sensordb.sensor (name, type, platform_id, manufacturer, model_number)
    VALUES ($1, $2, $3, NULL, NULL)
    ON CONFLICT (name, type, platform_id) DO UPDATE SET name = sensor.name
    RETURNING id, platform_id, name, type as "sensor_type: SensorType", manufacturer, model_number
    "#,
            sensor_name,
            sensor_type as SensorType,
            platform_id
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(sensor)
    }

    pub(crate) async fn register_or_get_campaign(
        &self,
        name: String,
    ) -> Result<CampaignEntry, Error> {
        let campaign = sqlx::query_as!(
            CampaignEntry,
            r#"
        INSERT INTO sensordb.campaign (name)
        VALUES ($1)
        ON CONFLICT (name) DO UPDATE SET name = campaign.name
        RETURNING id, name
        "#,
            name
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(campaign)
    }

    pub(crate) async fn register_or_get_mission(
        &self,
        campaign_id: CampaignId,
        name: String,
    ) -> Result<MissionEntry, Error> {
        let campaign_id: i32 = campaign_id.into();

        let mission = sqlx::query_as!(
            MissionEntry,
            r#"
    INSERT INTO sensordb.mission (campaign_id, name)
    VALUES ($1, $2)
    ON CONFLICT (name, campaign_id) DO UPDATE SET name = mission.name
    RETURNING id, name
    "#,
            campaign_id,
            name
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(mission)
    }

    pub(crate) async fn get_recording(
        &self,
        mission_id: MissionId,
        name: &String,
    ) -> Result<Option<RecordingEntry>, Error> {
        let mission_id: i32 = mission_id.into();

        let table_entry = sqlx::query_as!(
            RecordingEntry,
            r#"
        SELECT *
        FROM sensordb.recording
        WHERE mission_id = $1 AND name = $2
        "#,
            mission_id,
            name
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(table_entry)
    }

    pub(crate) async fn register_recording(
        &self,
        parent_recording_id: Option<RecordingId>,
        mission_id: MissionId,
        platform_id: Option<PlatformId>,
        sensor_id: Option<SensorId>,
        name: String,
        start_date_time: Option<DateTime<Utc>>,
        end_date_time: Option<DateTime<Utc>>,
    ) -> Result<RecordingEntry, Error> {
        let parent_recording_id: Option<i32> = parent_recording_id.map(|id| id.into());
        let platform_id: Option<i32> = platform_id.map(|id| id.into());
        let sensor_id: Option<i32> = sensor_id.map(|id| id.into());
        let mission_id: i32 = mission_id.into();
        let table_entry = sqlx::query_as!(
            RecordingEntry,
            r#"
        INSERT INTO sensordb.recording (parent_id, mission_id, platform_id, sensor_id, name, start_date, end_date)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id, parent_id, mission_id, platform_id, sensor_id, name, start_date, end_date
        "#,
            parent_recording_id,
            mission_id,
            platform_id,
            sensor_id,
            name,
            start_date_time,
            end_date_time
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(table_entry)
    }

    pub(crate) async fn register_trajectory(
        &self,
        recording_id: RecordingId,
        trajectory_domain: TrajectoryDomain,
        interpolation_type: InterpolationType,
        extrapolation_type: ExtrapolationType,
    ) -> Result<TrajectoryEntry, Error> {
        let recording_id: i32 = recording_id.into();

        let table_entry = sqlx::query_as!(
            TrajectoryEntry,
            r#"
        INSERT INTO sensordb.trajectory (recording_id, domain, interpolation_type, extrapolation_type)
        VALUES ($1, $2, $3, $4)
        RETURNING id, recording_id, domain AS "domain!: TrajectoryDomain", interpolation_type AS "interpolation_type!: InterpolationType", extrapolation_type AS "extrapolation_type!: ExtrapolationType"
        "#,
            recording_id,
            trajectory_domain as TrajectoryDomain,
            interpolation_type as InterpolationType,
            extrapolation_type as ExtrapolationType,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(table_entry)
    }

    pub(crate) async fn register_point_cloud(
        &self,
        recording_id: Option<RecordingId>,
        name: Option<String>,
        start_date_time: Option<DateTime<Utc>>,
        end_date_time: Option<DateTime<Utc>>,
    ) -> Result<PointCloudEntry, Error> {
        let recording_id: Option<i32> = recording_id.map(|x| x.into());
        let table_entry = sqlx::query_as!(
            PointCloudEntry,
            r#"
        INSERT INTO sensordb.point_cloud (recording_id, name, start_date, end_date)
        VALUES ($1, $2, $3, $4)
        RETURNING *
        "#,
            recording_id,
            name,
            start_date_time,
            end_date_time
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(table_entry)
    }
}
