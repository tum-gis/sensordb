use crate::Error;
use itertools::Itertools;
use sqlx::{Pool, Postgres};

pub async fn estimate_sensor_signatures(
    pool: &Pool<Postgres>,
    feature_id: i64,
    spherical_range_bin_boundaries: Vec<f64>,
    surface_zenith_angle_bin_boundaries: Vec<f64>,
    surface_azimuth_angle_bin_boundaries: Vec<f64>,
) -> Result<(), Error> {
    let spherical_range_bin_boundaries_index_max = spherical_range_bin_boundaries.len() - 2;
    let spherical_range_bin_cases_sql = spherical_range_bin_boundaries
        .windows(2)
        .enumerate()
        .map(|(i, b)| format!("WHEN s.id = {} THEN numrange({}, {})", i, b[0], b[1]))
        .join(" ");

    let surface_zenith_angle_bin_boundaries_index_max =
        surface_zenith_angle_bin_boundaries.len() - 2;
    let surface_zenith_angle_bin_boundaries_sql = surface_zenith_angle_bin_boundaries
        .windows(2)
        .enumerate()
        .map(|(i, b)| format!("WHEN s.id = {} THEN numrange({}, {})", i, b[0], b[1]))
        .join(" ");

    let surface_azimuth_angle_bin_boundaries_max = surface_azimuth_angle_bin_boundaries.len() - 2;
    let surface_azimuth_angle_bin_boundaries_sql = surface_azimuth_angle_bin_boundaries
        .windows(2)
        .enumerate()
        .map(|(i, b)| format!("WHEN s.id = {} THEN numrange({}, {})", i, b[0], b[1]))
        .join(" ");

    let query = format!("WITH
defined_spherical_range_bins AS (
    SELECT
        s.id AS spherical_range_bin_index,
        r.spherical_range_bin
    FROM generate_series(0, {spherical_range_bin_boundaries_index_max}) AS s(id)
    CROSS JOIN LATERAL (
        VALUES
            (CASE {spherical_range_bin_cases_sql} END)
    ) AS r(spherical_range_bin)
),
defined_surface_zenith_angle_bins AS (
    SELECT
        s.id AS surface_zenith_angle_bin_index,
        r.surface_zenith_angle_bin
    FROM generate_series(0, {surface_zenith_angle_bin_boundaries_index_max}) AS s(id)
    CROSS JOIN LATERAL (
        VALUES
            (CASE {surface_zenith_angle_bin_boundaries_sql} END)
    ) AS r(surface_zenith_angle_bin)
),
defined_surface_azimuth_angle_bins AS (
SELECT
    s.id AS surface_azimuth_angle_bin_index,
    r.surface_azimuth_angle_bin
FROM generate_series(0, {surface_azimuth_angle_bin_boundaries_max}) AS s(id)
    CROSS JOIN LATERAL (
        VALUES
            (CASE {surface_azimuth_angle_bin_boundaries_sql} END)
    ) AS r(surface_azimuth_angle_bin)
),
aggregated_data AS (
    SELECT
        sensor_id,
        campaign_id,
        (SELECT defined_spherical_range_bins.spherical_range_bin_index
         FROM defined_spherical_range_bins
         WHERE spherical_range::numeric <@ defined_spherical_range_bins.spherical_range_bin)                AS spherical_range_bin_index,
        (SELECT defined_spherical_range_bins.spherical_range_bin
         FROM defined_spherical_range_bins
         WHERE spherical_range::numeric <@ defined_spherical_range_bins.spherical_range_bin)                AS spherical_range_bin,
        (SELECT defined_surface_zenith_angle_bins.surface_zenith_angle_bin_index
         FROM defined_surface_zenith_angle_bins
         WHERE surface_zenith_angle::numeric <@ defined_surface_zenith_angle_bins.surface_zenith_angle_bin) AS surface_zenith_angle_bin_index,
        (SELECT defined_surface_zenith_angle_bins.surface_zenith_angle_bin
         FROM defined_surface_zenith_angle_bins
         WHERE surface_zenith_angle::numeric <@ defined_surface_zenith_angle_bins.surface_zenith_angle_bin) AS surface_zenith_angle_bin,
        (SELECT defined_surface_azimuth_angle_bins.surface_azimuth_angle_bin
         FROM defined_surface_azimuth_angle_bins
         WHERE surface_azimuth_angle::numeric <@ defined_surface_azimuth_angle_bins.surface_azimuth_angle_bin) AS surface_azimuth_angle_bin_index,
        (SELECT defined_surface_azimuth_angle_bins.surface_azimuth_angle_bin
         FROM defined_surface_azimuth_angle_bins
         WHERE surface_azimuth_angle::numeric <@ defined_surface_azimuth_angle_bins.surface_azimuth_angle_bin) AS surface_azimuth_angle_bin,
        intensity
    FROM sensordb.lidar_beam_enriched
    WHERE lidar_beam_enriched.feature_id = {feature_id}
),
stats AS (
    SELECT
         sensor_id,
         campaign_id,
         spherical_range_bin_index,
         spherical_range_bin,
         surface_zenith_angle_bin_index,
         surface_zenith_angle_bin,
         surface_azimuth_angle_bin_index,
         surface_azimuth_angle_bin,
         MIN(intensity) AS intensity_min,
         MAX(intensity) AS intensity_max,
         AVG(intensity) AS intensity_mean,
         STDDEV(intensity) AS intensity_stddev,
         PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY intensity) AS intensity_p5,
         PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY intensity) AS intensity_p10,
         PERCENTILE_CONT(0.20) WITHIN GROUP (ORDER BY intensity) AS intensity_p20,
         PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY intensity) AS intensity_p25,
         PERCENTILE_CONT(0.30) WITHIN GROUP (ORDER BY intensity) AS intensity_p30,
         PERCENTILE_CONT(0.40) WITHIN GROUP (ORDER BY intensity) AS intensity_p40,
         PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY intensity) AS intensity_median,
         PERCENTILE_CONT(0.60) WITHIN GROUP (ORDER BY intensity) AS intensity_p60,
         PERCENTILE_CONT(0.70) WITHIN GROUP (ORDER BY intensity) AS intensity_p70,
         PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY intensity) AS intensity_p75,
         PERCENTILE_CONT(0.80) WITHIN GROUP (ORDER BY intensity) AS intensity_p80,
         PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY intensity) AS intensity_p90,
         PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY intensity) AS intensity_p95,
         COUNT(intensity) AS points_count
   FROM aggregated_data
   GROUP BY sensor_id,
            campaign_id,
            spherical_range_bin_index,
            spherical_range_bin,
            surface_zenith_angle_bin_index,
            surface_zenith_angle_bin,
            surface_azimuth_angle_bin_index,
            surface_azimuth_angle_bin
   ),
signatures AS (
    INSERT INTO sensordb.feature_lidar_signature (feature_id, sensor_id, campaign_id)
    SELECT DISTINCT
        {feature_id} AS feature_id,
        sensor_id,
        campaign_id
    FROM stats
    RETURNING id, sensor_id, campaign_id
)
INSERT INTO sensordb.feature_lidar_signature_entry (
    feature_lidar_signature_id,
    spherical_range_bin,
    surface_zenith_angle_bin,
    surface_azimuth_angle_bin,
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
    points_count
)
SELECT
    sig.id AS feature_lidar_signature_id,
    s.spherical_range_bin,
    s.surface_zenith_angle_bin,
    s.surface_azimuth_angle_bin,
    s.intensity_mean,
    s.intensity_stddev,
    s.intensity_min,
    s.intensity_p5,
    s.intensity_p10,
    s.intensity_p20,
    s.intensity_p25,
    s.intensity_p30,
    s.intensity_p40,
    s.intensity_median,
    s.intensity_p60,
    s.intensity_p70,
    s.intensity_p75,
    s.intensity_p80,
    s.intensity_p90,
    s.intensity_p95,
    s.intensity_max,
    s.points_count
FROM stats s
JOIN signatures sig ON s.sensor_id = sig.sensor_id AND s.campaign_id = sig.campaign_id;");

    let _a = sqlx::query(&query).fetch_all(pool).await?;

    Ok(())
}

pub async fn enrich_by_sensor_signatures(pool: &Pool<Postgres>) -> Result<(), Error> {
    sqlx::query("WITH signatures AS (
    SELECT
      'sensordb_' ||
      'sensor_' || sensor_id ||
      '_campaign_' || campaign_id ||
      '_range_' || lower(feature_lidar_signature_entry.spherical_range_bin) || '_' || upper(feature_lidar_signature_entry.spherical_range_bin) ||
      '_surface_zenith_angle_' || round(degrees(lower(feature_lidar_signature_entry.surface_zenith_angle_bin))) || '_' || round(degrees(upper(feature_lidar_signature_entry.surface_zenith_angle_bin))) ||
      '_surface_azimuth_angle_' || round(degrees(lower(feature_lidar_signature_entry.surface_azimuth_angle_bin))) || '_' || round(degrees(upper(feature_lidar_signature_entry.surface_azimuth_angle_bin)))
          AS base_property_name,
        *
    FROM sensordb.feature_lidar_signature
    JOIN sensordb.feature_lidar_signature_entry ON feature_lidar_signature.id = feature_lidar_signature_entry.feature_lidar_signature_id
)
INSERT INTO citydb.property (feature_id, datatype_id, namespace_id, name, val_float8, val_int)
SELECT
    signatures.feature_id,
    v.datatype_id,
    3 AS namespace_id,
    signatures.base_property_name || '_' || v.metric_name AS name,
    v.val_float8,
    v.val_int
FROM signatures
CROSS JOIN LATERAL (
    VALUES
        ('intensity_min', 4, signatures.intensity_min, NULL),
        ('intensity_max', 4, signatures.intensity_max, NULL),
        ('intensity_mean', 4, signatures.intensity_mean, NULL),
        ('intensity_stddev', 4, signatures.intensity_stddev, NULL),
        ('intensity_p25', 4, signatures.intensity_p25, NULL),
        ('intensity_median', 4, signatures.intensity_median, NULL),
        ('intensity_p75', 4, signatures.intensity_p75, NULL),
        ('points_count', 3, NULL, signatures.points_count)
) AS v(metric_name, datatype_id, val_float8, val_int)
WHERE CASE
    WHEN v.datatype_id = 4 THEN v.val_float8 IS NOT NULL
    WHEN v.datatype_id = 3 THEN v.val_int IS NOT NULL
    ELSE FALSE
END;")
        .fetch_all(pool)
        .await?;

    Ok(())
}
