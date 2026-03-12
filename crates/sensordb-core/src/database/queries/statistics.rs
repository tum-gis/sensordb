use crate::Error;
use crate::database::datatype::{CampaignId, FeatureClassName, FeatureObjectName, SensorId};
use crate::io::statistics::{
    AssociatedPointCloudStatistics, CampaignGroupedStatistics,
    CampaignSphericalRangeGroupedStatistics,
    CityModelFeatureClassNameFeatureObjectNameGroupStatistics,
    CityModelFeatureClassNameGroupStatistics, FeatureClassNameGroupedStatistics,
    FeatureObjectNameGroupedStatistics, SensorGroupedStatistics,
};
use sqlx::{Pool, Postgres};
use std::collections::BTreeMap;

pub async fn get_city_model_feature_class_namer_stats(
    pool: &Pool<Postgres>,
    feature_class_name: FeatureClassName,
) -> Result<CityModelFeatureClassNameGroupStatistics, Error> {
    let number_of_features = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)
    FROM feature
    LEFT JOIN objectclass ON feature.objectclass_id = objectclass.id
    WHERE objectclass.classname = $1;",
    )
    .bind(&feature_class_name)
    .fetch_one(pool)
    .await? as u64;

    let stats_per_name: Vec<(Option<FeatureObjectName>, i64)> = sqlx::query_as(
        "SELECT
    feature_property_name.feature_object_name AS name,
    COUNT(feature.id) AS feature_count
FROM feature
LEFT JOIN (
    SELECT feature_id AS feature_object_id, val_string AS feature_object_name
    FROM property
    WHERE namespace_id = 1 AND name = 'name'
) AS feature_property_name ON feature.id = feature_property_name.feature_object_id
LEFT JOIN objectclass ON feature.objectclass_id = objectclass.id
WHERE objectclass.classname = $1
GROUP BY feature_property_name.feature_object_name
ORDER BY feature_property_name.feature_object_name;",
    )
    .bind(&feature_class_name)
    .fetch_all(pool)
    .await?;
    let group_feature_object_name: BTreeMap<
        FeatureObjectName,
        CityModelFeatureClassNameFeatureObjectNameGroupStatistics,
    > = stats_per_name
        .into_iter()
        .map(|x| {
            (
                x.0.unwrap_or(FeatureObjectName(String::new())),
                CityModelFeatureClassNameFeatureObjectNameGroupStatistics::new(x.1 as u64),
            )
        })
        .collect();

    let stats = CityModelFeatureClassNameGroupStatistics::new(
        number_of_features,
        group_feature_object_name,
    );
    Ok(stats)
}

pub async fn get_sensor_grouped_statistics(
    pool: &Pool<Postgres>,
    sensor_id: SensorId,
) -> Result<SensorGroupedStatistics, Error> {
    let counting_result: (i64, i64, i64) = sqlx::query_as(&format!(
        "SELECT
    COUNT(*) AS number_of_points,
    COUNT(feature_object_id) AS number_of_associated_points,
    COUNT(DISTINCT feature_object_id) AS number_of_associated_features
FROM sensordb.lidar_beam_enriched
WHERE sensor_id = {};",
        sensor_id,
    ))
    .fetch_one(pool)
    .await?;

    Ok(SensorGroupedStatistics::new(
        counting_result.0 as u64,
        counting_result.1 as u64,
        counting_result.2 as u64,
    ))
}

pub async fn get_campaign_grouped_statistics(
    pool: &Pool<Postgres>,
    campaign_id: CampaignId,
    spherical_range_bin_size: f64,
) -> Result<CampaignGroupedStatistics, Error> {
    let counting_result: (i64, i64, i64) = sqlx::query_as(&format!(
        "SELECT
    COUNT(*) AS number_of_points,
    COUNT(feature_object_id) AS number_of_associated_points,
    COUNT(DISTINCT feature_object_id) AS number_of_associated_features
FROM sensordb.lidar_beam_enriched
WHERE campaign_id = {};",
        campaign_id,
    ))
    .fetch_one(pool)
    .await?;

    let group_spherical_range_result: Vec<(i64, f64, f64, i64, i64, i64)> =
        sqlx::query_as(&format!(
            "WITH aggregated_data AS (
    SELECT
        floor(spherical_range / {spherical_range_bin_size})::INT8 AS spherical_range_bin_index,
        feature_object_id
    FROM sensordb.lidar_beam_enriched
    WHERE lidar_beam_enriched.campaign_id = {}
)
SELECT
    spherical_range_bin_index,
    (spherical_range_bin_index * {spherical_range_bin_size})::FLOAT8 AS spherical_range_bin_min,
    ((spherical_range_bin_index+1) * {spherical_range_bin_size})::FLOAT8 AS spherical_range_bin_max,
    COUNT(*) AS number_of_points,
    COUNT(feature_object_id) AS number_of_associated_points,
    COUNT(DISTINCT feature_object_id) AS number_of_associated_features
FROM aggregated_data
GROUP BY spherical_range_bin_index;",
            campaign_id
        ))
        .fetch_all(pool)
        .await?;

    let group_spherical_range: BTreeMap<u32, CampaignSphericalRangeGroupedStatistics> =
        group_spherical_range_result
            .into_iter()
            .map(|x| {
                (
                    x.0 as u32,
                    CampaignSphericalRangeGroupedStatistics::new(
                        x.1, x.2, x.3 as u64, x.4 as u64, x.5 as u64,
                    ),
                )
            })
            .collect();

    Ok(CampaignGroupedStatistics::new(
        counting_result.0 as u64,
        counting_result.1 as u64,
        counting_result.2 as u64,
        group_spherical_range,
    ))
}

pub async fn get_feature_class_name_grouped_statistics(
    pool: &Pool<Postgres>,
    feature_class_name: FeatureClassName,
) -> Result<FeatureClassNameGroupedStatistics, Error> {
    let number_of_features = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)
    FROM feature
    LEFT JOIN objectclass ON feature.objectclass_id = objectclass.id
    WHERE objectclass.classname = $1;",
    )
    .bind(&feature_class_name)
    .fetch_one(pool)
    .await? as u64;

    let number_of_associated_points = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)
FROM sensordb.lidar_beam_enriched
WHERE lidar_beam_enriched.feature_class_name = $1;",
    )
    .bind(&feature_class_name)
    .fetch_one(pool)
    .await? as u64;

    /*let counting_result: (i64, i64) = sqlx::query_as(&format!(
        "SELECT
    COUNT(feature_object_id) AS number_of_associated_points,
    COUNT(DISTINCT feature_object_id) AS number_of_associated_features
FROM sensordb.lidar_beam_enriched WHERE feature_class_name = '{}';",
        filter_by_feature_class_name
    ))
    .fetch_one(pool)
    .await?;
          let  number_of_features: u64 = counting_result.1 as u64;
        number_of_associated_points: u64,
    let counting_statistics_element = CountingStatisticsElement::from(counting_result);*/

    let stats_feature_object_name_grouped: BTreeMap<
        FeatureObjectName,
        FeatureObjectNameGroupedStatistics,
    > = BTreeMap::new();

    let counting_result: Vec<(CampaignId, SensorId, i64, i64)> = sqlx::query_as(&format!(
        "SELECT
    campaign_id,
    sensor_id,
    COUNT(*) AS number_of_associated_points,
    COUNT(DISTINCT feature_object_id) AS number_of_associated_features
FROM sensordb.lidar_beam_enriched WHERE feature_class_name = '{}'
GROUP BY campaign_id, sensor_id
ORDER BY campaign_id, sensor_id;
",
        feature_class_name
    ))
    .fetch_all(pool)
    .await?;

    let stats_campaign_sensor_grouped: BTreeMap<
        CampaignId,
        BTreeMap<SensorId, AssociatedPointCloudStatistics>,
    > = counting_result
        .into_iter()
        .map(
            |(
                campaign_id,
                sensor_id,
                number_of_associated_points,
                number_of_associated_features,
            )| {
                let counting_element = AssociatedPointCloudStatistics::new(
                    number_of_associated_points as u64,
                    number_of_associated_features as u64,
                );
                (campaign_id, sensor_id, counting_element)
            },
        )
        .fold(
            BTreeMap::new(),
            |mut acc, (campaign_id, sensor_id, counting_element)| {
                acc.entry(campaign_id)
                    .or_insert_with(BTreeMap::new)
                    .insert(sensor_id, counting_element);
                acc
            },
        );

    let counting_result: Vec<(Option<FeatureObjectName>, CampaignId, SensorId, i64, i64)> =
        sqlx::query_as(&format!(
            "SELECT
    feature_object_name,
    campaign_id,
    sensor_id,
    COUNT(*) AS number_of_associated_points,
    COUNT(DISTINCT feature_object_id) AS number_of_associated_features
FROM sensordb.lidar_beam_enriched WHERE feature_class_name = '{}'
GROUP BY feature_object_name, campaign_id, sensor_id
ORDER BY feature_object_name, campaign_id, sensor_id;
",
            feature_class_name
        ))
        .fetch_all(pool)
        .await?;

    let stats_feature_object_name_campaign_sensor_grouped: BTreeMap<
        FeatureObjectName,
        BTreeMap<CampaignId, BTreeMap<SensorId, AssociatedPointCloudStatistics>>,
    > = counting_result
        .into_iter()
        .map(
            |(
                feature_object_name,
                campaign_id,
                sensor_id,
                number_of_associated_points,
                number_of_associated_features,
            )| {
                let counting_element = AssociatedPointCloudStatistics::new(
                    number_of_associated_points as u64,
                    number_of_associated_features as u64,
                );
                (
                    feature_object_name.unwrap_or(FeatureObjectName(String::new())),
                    campaign_id,
                    sensor_id,
                    counting_element,
                )
            },
        )
        .fold(
            BTreeMap::new(),
            |mut acc, (feature_object_name, campaign_id, sensor_id, counting_element)| {
                acc.entry(feature_object_name)
                    .or_insert_with(BTreeMap::new)
                    .entry(campaign_id)
                    .or_insert_with(BTreeMap::new)
                    .insert(sensor_id, counting_element);
                acc
            },
        );

    let stats = FeatureClassNameGroupedStatistics::new(
        number_of_features,
        number_of_associated_points,
        stats_feature_object_name_grouped,
        stats_campaign_sensor_grouped,
        stats_feature_object_name_campaign_sensor_grouped,
    );
    Ok(stats)
}
