use crate::Error;
use crate::database::datatype::{
    CampaignId, FeatureClassName, FeatureId, FeatureObjectName, SensorId,
};
use crate::database::manager::DatabaseManager;
use crate::database::queries::enrichment::{
    enrich_by_sensor_signatures, estimate_sensor_signatures,
};
use crate::database::queries::statistics::get_campaign_grouped_statistics;
use crate::database::queries::statistics::get_city_model_feature_class_namer_stats;
use crate::database::queries::statistics::get_feature_class_name_grouped_statistics;
use crate::database::queries::statistics::get_sensor_grouped_statistics;
use crate::database::util::get_progress_bar;
use crate::io::statistics::{
    CampaignGroupedStatistics, CityModelFeatureClassNameGroupStatistics, CityModelStatistics,
    FeatureClassNameGroupedStatistics, Overview, SensorGroupedStatistics,
};
use crate::io::statistics::{SensorStatistics, StatisticsDocument};
use nalgebra::Point3;
use polars::prelude::{CsvWriter, DataFrame, NamedFrom, SerWriter, Series};
use rayon::iter::ParallelIterator;
use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use tokio::task::JoinHandle;
use tracing::info;

impl DatabaseManager {
    pub async fn write_statistics(
        &self,
        output_statistics_path: impl AsRef<Path>,
        spherical_range_bin_size: f64,
    ) -> Result<(), Error> {
        info!("[1/7] ⬇️  Retrieve overview");
        let overview = self.get_overview().await?;

        info!("[2/7] ⬇️  Calculate city model statistics");
        let city_model_stats = self.calculate_city_model_stats(&overview).await?;

        info!("[3/7] ⬇️  Calculate sensor statistics");
        let sensor_stats = Some(
            self.calculate_sensor_stats(&overview, spherical_range_bin_size)
                .await?,
        );
        // let sensor_stats = None;

        let statistics_document = StatisticsDocument::new(overview, city_model_stats, sensor_stats);
        let statistics_document_json = serde_json::to_string_pretty(&statistics_document)?;
        std::fs::write(output_statistics_path, statistics_document_json)?;
        Ok(())
    }

    async fn get_overview(&self) -> Result<Overview, Error> {
        let sensors: Vec<(SensorId, String)> =
            sqlx::query_as("SELECT id, name FROM sensordb.sensor ORDER BY id;")
                .fetch_all(&self.pool)
                .await?;
        let campaigns: Vec<(CampaignId, String)> =
            sqlx::query_as("SELECT id, name FROM sensordb.campaign ORDER BY id;")
                .fetch_all(&self.pool)
                .await?;
        let feature_class_names: Vec<(i32, FeatureClassName)> =
            sqlx::query_as("SELECT id, classname FROM citydb.objectclass ORDER BY id;")
                .fetch_all(&self.pool)
                .await?;
        let feature_object_ids: Vec<(FeatureId, String)> =
            sqlx::query_as("SELECT id, objectid FROM citydb.feature ORDER BY id;")
                .fetch_all(&self.pool)
                .await?;

        let overview = Overview::new(
            sensors.into_iter().collect(),
            campaigns.into_iter().collect(),
            feature_class_names.into_iter().map(|x| x.1).collect(),
            feature_object_ids.into_iter().collect(),
        );
        Ok(overview)
    }

    async fn calculate_city_model_stats(
        &self,
        overview: &Overview,
    ) -> Result<CityModelStatistics, Error> {
        let number_of_features = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM feature;")
            .fetch_one(&self.pool)
            .await? as u64;

        let progress_bar = get_progress_bar(
            overview.feature_class_names.len() as u64,
            "[5/7] 💥🔦 Retrieve statistics per feature class names",
        );
        let mut handles: Vec<
            JoinHandle<Result<(FeatureClassName, CityModelFeatureClassNameGroupStatistics), Error>>,
        > = vec![];
        for current_feature_class_name in &overview.feature_class_names {
            let future_progress_bar = progress_bar.clone();
            let future_pool = self.pool.clone();
            let future_feature_class_name = current_feature_class_name.clone();

            let current_handle = tokio::spawn(async move {
                let stats = get_city_model_feature_class_namer_stats(
                    &future_pool,
                    future_feature_class_name.clone(),
                )
                .await?;
                future_progress_bar.inc(1);

                Ok((future_feature_class_name, stats))
            });
            handles.push(current_handle);
        }
        let mut group_feature_class_name_stats: BTreeMap<
            FeatureClassName,
            CityModelFeatureClassNameGroupStatistics,
        > = BTreeMap::new();
        for current_handle in handles {
            let result = current_handle.await.unwrap().expect("TODO: panic message");
            group_feature_class_name_stats.insert(result.0, result.1);
        }
        progress_bar.finish();

        let stats = CityModelStatistics::new(number_of_features, group_feature_class_name_stats);
        Ok(stats)
    }

    async fn calculate_sensor_stats(
        &self,
        overview: &Overview,
        spherical_range_bin_size: f64,
    ) -> Result<SensorStatistics, Error> {
        let progress_bar = get_progress_bar(
            overview.sensors.len() as u64,
            "[3/7] 💥🔦 Retrieve statistics per sensor",
        );
        let number_of_associated_features = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(DISTINCT feature_object_id)
FROM sensordb.lidar_beam_enriched;",
        )
        .fetch_one(&self.pool)
        .await? as u64;

        let mut handles: Vec<JoinHandle<Result<(SensorId, SensorGroupedStatistics), Error>>> =
            vec![];
        for current_sensor_id in overview.sensors.keys() {
            let future_progress_bar = progress_bar.clone();
            let future_pool = self.pool.clone();
            let future_sensor_id = *current_sensor_id;

            let current_handle = tokio::spawn(async move {
                let stats = get_sensor_grouped_statistics(&future_pool, future_sensor_id).await?;
                future_progress_bar.inc(1);

                Ok((future_sensor_id, stats))
            });
            handles.push(current_handle);
        }
        let mut group_sensor_stats: BTreeMap<SensorId, SensorGroupedStatistics> = BTreeMap::new();
        for current_handle in handles {
            let result = current_handle.await.unwrap().expect("TODO: panic message");
            group_sensor_stats.insert(result.0, result.1);
        }
        progress_bar.finish();

        let progress_bar = get_progress_bar(
            overview.sensors.len() as u64,
            "[4/7] 💥🔦 Retrieve statistics per campaign",
        );
        let mut handles: Vec<JoinHandle<Result<(CampaignId, CampaignGroupedStatistics), Error>>> =
            vec![];
        for current_campaign_id in overview.campaigns.keys() {
            let future_progress_bar = progress_bar.clone();
            let future_pool = self.pool.clone();
            let future_campaign_id = *current_campaign_id;

            let current_handle = tokio::spawn(async move {
                let stats = get_campaign_grouped_statistics(
                    &future_pool,
                    future_campaign_id,
                    spherical_range_bin_size,
                )
                .await?;
                future_progress_bar.inc(1);

                Ok((future_campaign_id, stats))
            });
            handles.push(current_handle);
        }
        let mut group_campaign_stats: BTreeMap<CampaignId, CampaignGroupedStatistics> =
            BTreeMap::new();
        for current_handle in handles {
            let result = current_handle.await.unwrap().expect("TODO: panic message");
            group_campaign_stats.insert(result.0, result.1);
        }
        progress_bar.finish();

        let progress_bar = get_progress_bar(
            overview.feature_class_names.len() as u64,
            "[5/7] 💥🔦 Retrieve statistics per feature class names",
        );
        let mut handles: Vec<
            JoinHandle<Result<(FeatureClassName, FeatureClassNameGroupedStatistics), Error>>,
        > = vec![];
        for current_feature_class_name in &overview.feature_class_names {
            let future_progress_bar = progress_bar.clone();
            let future_pool = self.pool.clone();
            let future_feature_class_name = current_feature_class_name.clone();

            let current_handle = tokio::spawn(async move {
                let stats = get_feature_class_name_grouped_statistics(
                    &future_pool,
                    future_feature_class_name.clone(),
                )
                .await?;
                future_progress_bar.inc(1);

                Ok((future_feature_class_name, stats))
            });
            handles.push(current_handle);
        }
        let mut group_feature_class_name_stats: BTreeMap<
            FeatureClassName,
            FeatureClassNameGroupedStatistics,
        > = BTreeMap::new();
        for current_handle in handles {
            let result = current_handle.await.unwrap().expect("TODO: panic message");
            if result.1.number_of_features > 0 {
                group_feature_class_name_stats.insert(result.0, result.1);
            }
        }
        progress_bar.finish();

        let statistics = SensorStatistics::new(
            number_of_associated_features,
            group_sensor_stats,
            group_campaign_stats,
            group_feature_class_name_stats,
        );
        Ok(statistics)
    }
}

impl DatabaseManager {
    pub(crate) fn construct_where_clause(
        &self,
        corner_min: Option<Point3<f64>>,
        corner_max: Option<Point3<f64>>,
    ) -> String {
        let mut where_clause = String::new();
        if corner_min.is_some() || corner_max.is_some() {
            where_clause.push('(');
        }
        if let Some(corner_min) = corner_min {
            where_clause.push_str(&format!(
                "({} <= ST_XMax(feature.envelope) AND {} <= ST_YMax(feature.envelope) AND {} <= ST_ZMax(feature.envelope))",
                corner_min.x, corner_min.y, corner_min.z
            ));
        }
        if corner_min.is_some() && corner_max.is_some() {
            where_clause.push_str(" AND ");
        }
        if let Some(corner_max) = corner_max {
            where_clause.push_str(&format!(
                "(ST_XMin(feature.envelope) <= {} AND ST_YMin(feature.envelope) <= {} AND ST_ZMin(feature.envelope) <= {})",
                corner_max.x, corner_max.y, corner_max.z
            ));
        }
        if corner_min.is_some() || corner_max.is_some() {
            where_clause.push(')');
        }

        where_clause
    }

    pub async fn crop_city_model(
        &self,
        corner_min: Option<Point3<f64>>,
        corner_max: Option<Point3<f64>>,
    ) -> Result<(), Error> {
        let number_of_features_before =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM feature;")
                .fetch_one(&self.pool)
                .await? as u64;
        info!(
            "Number of features originally: {}",
            number_of_features_before
        );

        let where_clause = self.construct_where_clause(corner_min, corner_max);

        sqlx::query(&format!(
            "SELECT delete_feature(array_agg(id)) FROM feature WHERE NOT {};",
            where_clause
        ))
        .fetch_all(&self.pool)
        .await?;

        let number_of_features_after = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM feature;")
            .fetch_one(&self.pool)
            .await? as u64;
        info!(
            "Number of features after cropping: {}",
            number_of_features_after
        );
        info!(
            "Number of deleted features: {}",
            number_of_features_before - number_of_features_after
        );

        Ok(())
    }

    pub async fn export_feature_list(
        &self,
        output_list_path: impl AsRef<Path>,
        corner_min: Option<Point3<f64>>,
        corner_max: Option<Point3<f64>>,
    ) -> Result<(), Error> {
        let mut where_clause = self.construct_where_clause(corner_min, corner_max);
        if corner_min.is_some() || corner_max.is_some() {
            where_clause.push_str("WHERE ");
        }

        let feature_object_ids: Vec<(
            FeatureId,
            String,
            FeatureClassName,
            Option<FeatureObjectName>,
            Option<String>,
        )> = sqlx::query_as(&format!(
            "SELECT feature.id,
       feature.objectid,
       objectclass.classname,
       feature_property_name.feature_object_name,
       feature_property_lane_type.opendrive_lane_type
FROM citydb.feature
LEFT JOIN objectclass ON feature.objectclass_id = objectclass.id
LEFT JOIN (
    SELECT feature_id AS feature_object_id, val_string AS feature_object_name
    FROM property
    WHERE namespace_id = 1 AND name = 'name'
) AS feature_property_name ON feature.id = feature_property_name.feature_object_id
LEFT JOIN (
    SELECT feature_id AS feature_object_id, val_string AS opendrive_lane_type
    FROM property
    WHERE name = 'opendrive_lane_type'
) AS feature_property_lane_type ON feature.id = feature_property_lane_type.feature_object_id {};",
            where_clause
        ))
        .fetch_all(&self.pool)
        .await?;

        // Extract the ids and objectids into separate vectors
        let ids = feature_object_ids
            .iter()
            .map(|(id, _, _, _, _)| id.0)
            .collect::<Vec<i64>>();

        let feature_object_id_vec = feature_object_ids
            .iter()
            .map(|(_, objectid, _, _, _)| objectid.clone())
            .collect::<Vec<String>>();

        let feature_class_name_vec = feature_object_ids
            .iter()
            .map(|(_, _, x, _, _)| x.to_string())
            .collect::<Vec<String>>();

        let feature_object_name_vec = feature_object_ids
            .iter()
            .map(|(_, _, _, x, _)| x.clone().map(|x| x.to_string()))
            .collect::<Vec<Option<String>>>();

        let property_lane_type = feature_object_ids
            .iter()
            .map(|(_, _, _, _, x)| x.clone().map(|x| x.to_string()))
            .collect::<Vec<Option<String>>>();

        // Create a Polars DataFrame from these vectors
        let mut df = DataFrame::new(vec![
            Series::new("id".into(), ids).into(),
            Series::new("feature_object_id".into(), feature_object_id_vec).into(),
            Series::new("feature_class_name".into(), feature_class_name_vec).into(),
            Series::new("feature_object_name".into(), feature_object_name_vec).into(),
            Series::new("property_lane_type".into(), property_lane_type).into(),
        ])?;

        let mut file = std::fs::File::create(&output_list_path)?;
        CsvWriter::new(&mut file)
            .include_header(true)
            .with_separator(b';')
            .finish(&mut df)?;
        info!(
            "CSV file was successfully written to {}",
            PathBuf::from(output_list_path.as_ref()).display()
        );

        Ok(())
    }

    pub async fn enrich_city_model_by_signatures(&self) -> Result<(), Error> {
        info!("[1/2] ❌  Deleting enriched feature properties from citydb");
        self.clean_citydb_enriched_properties().await?;

        info!("[2/2]  ⬇️️☁️ Enriching citydb features with signatures");
        enrich_by_sensor_signatures(&self.pool).await?;

        info!("City model was successfully enriched by signatures with generic attributes");
        Ok(())
    }
}

impl DatabaseManager {
    pub async fn estimate_signatures(
        &self,
        spherical_range_bin_boundaries: Vec<f64>,
        surface_zenith_angle_bin_boundaries: Vec<f64>,
        surface_azimuth_angle_bin_boundaries: Vec<f64>,
    ) -> Result<(), Error> {
        info!("[1/2] ❌  Deleting signature tables");
        self.clean_signature_tables().await?;

        let feature_ids: Vec<i64> = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT DISTINCT feature_id FROM sensordb.lidar_beam_enriched;",
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .flatten()
        .collect();

        let progress_bar = get_progress_bar(
            feature_ids.len() as u64,
            "[2/2]  ⬇️️☁️ Enriching citydb features with properties",
        );

        let mut handles: Vec<JoinHandle<()>> = vec![];
        for current_feature_id in feature_ids.into_iter() {
            let future_progress_bar = progress_bar.clone();
            let future_pool = self.pool.clone();
            let future_spherical_range_bin_boundaries = spherical_range_bin_boundaries.clone();
            let future_surface_zenith_angle_bin_boundaries =
                surface_zenith_angle_bin_boundaries.clone();
            let future_surface_azimuth_angle_bin_boundaries =
                surface_azimuth_angle_bin_boundaries.clone();

            let current_handle = tokio::spawn(async move {
                estimate_sensor_signatures(
                    &future_pool,
                    current_feature_id,
                    future_spherical_range_bin_boundaries,
                    future_surface_zenith_angle_bin_boundaries,
                    future_surface_azimuth_angle_bin_boundaries,
                )
                .await
                .expect("should work");
                future_progress_bar.inc(1);
            });

            handles.push(current_handle);
        }
        for current_handle in handles {
            current_handle.await.unwrap();
        }
        progress_bar.finish();

        // info!("City model feature object ids: {:?}", feature_object_ids);

        Ok(())
    }
}
