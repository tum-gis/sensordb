use crate::database::datatype::{Namespace, PointCloudCellDataName};
use crate::{DatabaseManager, Error};
use indicatif::{ProgressBar, ProgressStyle};

impl DatabaseManager {
    pub async fn clean_all(&self) -> Result<(), Error> {
        let spinner_style = ProgressStyle::with_template("{prefix:.bold.dim} {spinner} {wide_msg}")
            .unwrap()
            .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ");

        let progress_bar = ProgressBar::new(4);
        progress_bar.set_style(spinner_style.clone());

        progress_bar.set_message("[1/3] ❌  Deleting association entries");
        self.clean_associations().await?;
        progress_bar.inc(1);

        progress_bar.set_message("[2/3] ❌  Deleting sensor view entries");
        self.clean_sensor_views().await?;
        progress_bar.inc(1);

        progress_bar.set_message("[3/3] ❌  Deleting remaining entries");
        sqlx::query(
            "TRUNCATE TABLE \
            sensordb.point_cloud_cell_data,\
            sensordb.point_cloud_cell,\
            sensordb.sensor, \
            sensordb.campaign, \
            sensordb.platform \
            RESTART IDENTITY CASCADE;",
        )
        .fetch_all(&self.pool)
        .await?;

        progress_bar.inc(1);

        progress_bar.finish_with_message("[3/3] ❌  Cleaned all tables");
        Ok(())
    }

    pub async fn clean_sensor_views(&self) -> Result<(), Error> {
        sqlx::query(
            "DELETE FROM sensordb.point_cloud_cell_data
WHERE namespace_id = $1 AND name IN ($2, $3, $4, $5, $6, $7, $8, $9, $10, $11);",
        )
        .bind(Namespace::Core)
        .bind(PointCloudCellDataName::BeamLine.as_ref())
        .bind(PointCloudCellDataName::BeamDirection.as_ref())
        .bind(PointCloudCellDataName::SphericalAzimuth.as_ref())
        .bind(PointCloudCellDataName::SphericalElevation.as_ref())
        .bind(PointCloudCellDataName::SphericalRange.as_ref())
        .bind(PointCloudCellDataName::ReflectionUncertaintyLine.as_ref())
        .bind(PointCloudCellDataName::ReflectionEnvelope.as_ref())
        .bind(PointCloudCellDataName::SensorPositionEnvelope.as_ref())
        .bind(PointCloudCellDataName::SensorPosition.as_ref())
        .bind(PointCloudCellDataName::SensorOrientation.as_ref())
        .fetch_all(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn clean_associations(&self) -> Result<(), Error> {
        sqlx::query(
            "TRUNCATE TABLE \
            sensordb.feature_geometry_data
            RESTART IDENTITY CASCADE;",
        )
        .fetch_all(&self.pool)
        .await?;

        sqlx::query(
            "DELETE FROM sensordb.point_cloud_cell_data
WHERE namespace_id = $1 AND name IN ($2, $3, $4, $5, $6, $7);",
        )
        .bind(Namespace::Core)
        .bind(PointCloudCellDataName::FeatureGeometryId.as_ref())
        .bind(PointCloudCellDataName::ReflectionPointSurfaceDistance.as_ref())
        .bind(PointCloudCellDataName::BeamLineSurfaceDistance.as_ref())
        .bind(PointCloudCellDataName::SurfaceZenithAngle.as_ref())
        .bind(PointCloudCellDataName::SurfaceAzimuthAngle.as_ref())
        .bind(PointCloudCellDataName::ReflectionLinePlaneIntersectionParameter.as_ref())
        .fetch_all(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn clean_citydb_enriched_properties(&self) -> Result<(), Error> {
        sqlx::query("DELETE FROM citydb.property WHERE name LIKE 'sensordb_%';")
            .fetch_all(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn clean_signature_tables(&self) -> Result<(), Error> {
        sqlx::query(
            "TRUNCATE TABLE \
            sensordb.feature_lidar_signature, \
            sensordb.feature_lidar_signature_entry \
            RESTART IDENTITY CASCADE;",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(())
    }
}
