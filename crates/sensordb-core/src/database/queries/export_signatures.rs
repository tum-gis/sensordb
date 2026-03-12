use crate::Error;
use polars::prelude::*;
use sqlx::{FromRow, Pool, Postgres};

#[derive(FromRow, Clone, Debug, PartialEq, PartialOrd)]
struct FeatureSignatureEntry {
    feature_object_id: String,
    feature_object_name: Option<String>,
    feature_class_name: String,
    sensor_id: i32,
    sensor_name: String,
    campaign_id: i32,
    campaign_name: String,
    spherical_range_bin_lower: f64,
    spherical_range_bin_upper: f64,
    surface_zenith_angle_bin_lower: f64,
    surface_zenith_angle_bin_upper: f64,
    surface_azimuth_angle_bin_lower: f64,
    surface_azimuth_angle_bin_upper: f64,
    intensity_mean: f64,
    intensity_stddev: Option<f64>,
    intensity_min: f64,
    intensity_p5: f64,
    intensity_p10: f64,
    intensity_p20: f64,
    intensity_p25: f64,
    intensity_p30: f64,
    intensity_p40: f64,
    intensity_median: f64,
    intensity_p60: f64,
    intensity_p70: f64,
    intensity_p75: f64,
    intensity_p80: f64,
    intensity_p90: f64,
    intensity_p95: f64,
    intensity_max: f64,
    intensity_iqr: f64,
    points_count: i32,
}

pub async fn export_signatures(pool: &Pool<Postgres>) -> Result<DataFrame, Error> {
    let entries: Vec<FeatureSignatureEntry> = sqlx::query_as("SELECT
    feature_object_id,
    feature_object_name,
    feature_class_name,
    sensor_id,
    sensor.name AS sensor_name,
    campaign_id,
    campaign.name AS campaign_name,
    lower(spherical_range_bin)::FLOAT AS spherical_range_bin_lower,
    upper(spherical_range_bin)::FLOAT AS spherical_range_bin_upper,
    lower(surface_zenith_angle_bin)::FLOAT AS surface_zenith_angle_bin_lower,
    upper(surface_zenith_angle_bin)::FLOAT AS surface_zenith_angle_bin_upper,
    lower(surface_azimuth_angle_bin)::FLOAT AS surface_azimuth_angle_bin_lower,
    upper(surface_azimuth_angle_bin)::FLOAT AS surface_azimuth_angle_bin_upper,
    intensity_mean,
    intensity_stddev,
    intensity_min,
    intensity_p5,
    intensity_p10,
    intensity_p20,
    intensity_p25,
    intensity_p30,
    intensity_p40,
    intensity_median,
    intensity_p60,
    intensity_p70,
    intensity_p75,
    intensity_p80,
    intensity_p90,
    intensity_p95,
    intensity_max,
    intensity_iqr,
    points_count
FROM sensordb.feature_lidar_signature
JOIN sensordb.campaign ON feature_lidar_signature.campaign_id = campaign.id
JOIN sensordb.sensor ON feature_lidar_signature.sensor_id = sensor.id
JOIN sensordb.feature_lidar_signature_entry ON feature_lidar_signature.id = feature_lidar_signature_entry.feature_lidar_signature_id
JOIN (
    SELECT
        feature.id AS feature_id,
        objectid AS feature_object_id,
        feature_property_name.feature_object_name AS feature_object_name,
        objectclass.classname AS feature_class_name
    FROM citydb.feature
    LEFT JOIN citydb.objectclass ON feature.objectclass_id = objectclass.id
    LEFT JOIN (
        SELECT feature_id AS feature_object_id, val_string AS feature_object_name
        FROM property
        WHERE namespace_id = 1 AND name = 'name'
    ) AS feature_property_name ON feature.id = feature_property_name.feature_object_id
) AS feature_info ON feature_lidar_signature.feature_id = feature_info.feature_id;")
    .fetch_all(pool)
    .await?;

    let feature_object_id_col: Vec<String> = entries
        .iter()
        .map(|f| f.feature_object_id.clone())
        .collect();
    let feature_object_name_col: Vec<Option<String>> = entries
        .iter()
        .map(|f| f.feature_object_name.clone())
        .collect();
    let feature_class_name_col: Vec<String> = entries
        .iter()
        .map(|f| f.feature_class_name.clone())
        .collect();
    let sensor_id_col: Vec<i32> = entries.iter().map(|f| f.sensor_id).collect();
    let sensor_name_col: Vec<String> = entries.iter().map(|f| f.sensor_name.clone()).collect();
    let campaign_id_col: Vec<i32> = entries.iter().map(|f| f.campaign_id).collect();
    let campaign_name_col: Vec<String> = entries.iter().map(|f| f.campaign_name.clone()).collect();
    let spherical_range_bin_lower_col: Vec<f64> = entries
        .iter()
        .map(|f| f.spherical_range_bin_lower)
        .collect();
    let spherical_range_bin_upper_col: Vec<f64> = entries
        .iter()
        .map(|f| f.spherical_range_bin_upper)
        .collect();
    let surface_zenith_angle_bin_lower_col: Vec<f64> = entries
        .iter()
        .map(|f| f.surface_zenith_angle_bin_lower)
        .collect();
    let surface_zenith_angle_bin_upper_col: Vec<f64> = entries
        .iter()
        .map(|f| f.surface_zenith_angle_bin_upper)
        .collect();
    let surface_azimuth_angle_bin_lower_col: Vec<f64> = entries
        .iter()
        .map(|f| f.surface_azimuth_angle_bin_lower)
        .collect();
    let surface_azimuth_angle_bin_upper_col: Vec<f64> = entries
        .iter()
        .map(|f| f.surface_azimuth_angle_bin_upper)
        .collect();
    let intensity_mean_col: Vec<f64> = entries.iter().map(|f| f.intensity_mean).collect();
    let intensity_stddev_col: Vec<Option<f64>> =
        entries.iter().map(|f| f.intensity_stddev).collect();
    let intensity_min_col: Vec<f64> = entries.iter().map(|f| f.intensity_min).collect();
    let intensity_p5_col: Vec<f64> = entries.iter().map(|f| f.intensity_p5).collect();
    let intensity_p10_col: Vec<f64> = entries.iter().map(|f| f.intensity_p10).collect();
    let intensity_p20_col: Vec<f64> = entries.iter().map(|f| f.intensity_p20).collect();
    let intensity_p25_col: Vec<f64> = entries.iter().map(|f| f.intensity_p25).collect();
    let intensity_p30_col: Vec<f64> = entries.iter().map(|f| f.intensity_p30).collect();
    let intensity_p40_col: Vec<f64> = entries.iter().map(|f| f.intensity_p40).collect();
    let intensity_median_col: Vec<f64> = entries.iter().map(|f| f.intensity_median).collect();
    let intensity_p60_col: Vec<f64> = entries.iter().map(|f| f.intensity_p60).collect();
    let intensity_p70_col: Vec<f64> = entries.iter().map(|f| f.intensity_p70).collect();
    let intensity_p75_col: Vec<f64> = entries.iter().map(|f| f.intensity_p75).collect();
    let intensity_p80_col: Vec<f64> = entries.iter().map(|f| f.intensity_p80).collect();
    let intensity_p90_col: Vec<f64> = entries.iter().map(|f| f.intensity_p90).collect();
    let intensity_p95_col: Vec<f64> = entries.iter().map(|f| f.intensity_p95).collect();
    let intensity_max_col: Vec<f64> = entries.iter().map(|f| f.intensity_max).collect();
    let intensity_iqr_col: Vec<f64> = entries.iter().map(|f| f.intensity_iqr).collect();
    let points_count_col: Vec<i32> = entries.iter().map(|f| f.points_count).collect();

    let df = DataFrame::new(vec![
        Series::new("feature_object_id".into(), feature_object_id_col).into_column(),
        Series::new("feature_object_name".into(), feature_object_name_col).into_column(),
        Series::new("feature_class_name".into(), feature_class_name_col).into_column(),
        Series::new("sensor_id".into(), sensor_id_col).into_column(),
        Series::new("sensor_name".into(), sensor_name_col).into_column(),
        Series::new("campaign_id".into(), campaign_id_col).into_column(),
        Series::new("campaign_name".into(), campaign_name_col).into_column(),
        Series::new(
            "spherical_range_bin_lower".into(),
            spherical_range_bin_lower_col,
        )
        .into_column(),
        Series::new(
            "spherical_range_bin_upper".into(),
            spherical_range_bin_upper_col,
        )
        .into_column(),
        Series::new(
            "surface_zenith_angle_bin_lower".into(),
            surface_zenith_angle_bin_lower_col,
        )
        .into_column(),
        Series::new(
            "surface_zenith_angle_bin_upper".into(),
            surface_zenith_angle_bin_upper_col,
        )
        .into_column(),
        Series::new(
            "surface_azimuth_angle_bin_lower".into(),
            surface_azimuth_angle_bin_lower_col,
        )
        .into_column(),
        Series::new(
            "surface_azimuth_angle_bin_upper".into(),
            surface_azimuth_angle_bin_upper_col,
        )
        .into_column(),
        Series::new("intensity_mean".into(), intensity_mean_col).into_column(),
        Series::new("intensity_stddev".into(), intensity_stddev_col).into_column(),
        Series::new("intensity_min".into(), intensity_min_col).into_column(),
        Series::new("intensity_p5".into(), intensity_p5_col).into_column(),
        Series::new("intensity_p10".into(), intensity_p10_col).into_column(),
        Series::new("intensity_p20".into(), intensity_p20_col).into_column(),
        Series::new("intensity_p25".into(), intensity_p25_col).into_column(),
        Series::new("intensity_p30".into(), intensity_p30_col).into_column(),
        Series::new("intensity_p40".into(), intensity_p40_col).into_column(),
        Series::new("intensity_median".into(), intensity_median_col).into_column(),
        Series::new("intensity_p60".into(), intensity_p60_col).into_column(),
        Series::new("intensity_p70".into(), intensity_p70_col).into_column(),
        Series::new("intensity_p75".into(), intensity_p75_col).into_column(),
        Series::new("intensity_p80".into(), intensity_p80_col).into_column(),
        Series::new("intensity_p90".into(), intensity_p90_col).into_column(),
        Series::new("intensity_p95".into(), intensity_p95_col).into_column(),
        Series::new("intensity_max".into(), intensity_max_col).into_column(),
        Series::new("intensity_iqr".into(), intensity_iqr_col).into_column(),
        Series::new("points_count".into(), points_count_col).into_column(),
    ])?;

    Ok(df)
}
