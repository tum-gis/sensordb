use crate::Error;
use crate::database::datatype::{DataType, PointCloudCellDataName, PointCloudCellId, TrajectoryId};
use sqlx::{FromRow, Pool, Postgres};

pub async fn compute_sensor_poses(
    pool: &Pool<Postgres>,
    point_cloud_cell_id: PointCloudCellId,
) -> Result<(), Error> {
    let point_cloud_cell_id: i32 = point_cloud_cell_id.into();

    #[derive(FromRow)]
    pub struct RecordingEntry {
        pub id: TrajectoryId,
    }

    let table_entry: Option<RecordingEntry> = sqlx::query_as!(
        RecordingEntry,
        r#"
        SELECT trajectory.id AS id
FROM sensordb.point_cloud_cell
JOIN sensordb.point_cloud ON point_cloud_cell.point_cloud_id=point_cloud.id
JOIN sensordb.trajectory ON point_cloud.recording_id=trajectory.recording_id
WHERE point_cloud_cell.id=$1;
        "#,
        point_cloud_cell_id
    )
    .fetch_optional(pool)
    .await?;

    let trajectory_id: i32 = table_entry.unwrap().id.into();

    let name_reflection_point = PointCloudCellDataName::ReflectionPoint.as_ref();
    let name_timestamp_second = PointCloudCellDataName::TimestampSecond.as_ref();
    let name_timestamp_nano_second = PointCloudCellDataName::TimestampNanoSecond.as_ref();
    let name_sensor_position = PointCloudCellDataName::SensorPosition.as_ref();
    let name_sensor_orientation = PointCloudCellDataName::SensorOrientation.as_ref();

    let query = format!("INSERT INTO sensordb.point_cloud_cell_data
    (point_cloud_cell_id, datatype_id, namespace_id, name, val_geometry_multi_point, val_quaternion_array)
WITH reflection_point AS (
    SELECT
      (dumped).path[1] AS point_index
    FROM sensordb.point_cloud_cell_data,
         LATERAL ST_Dump(val_geometry_multi_point) AS dumped
    WHERE point_cloud_cell_id = {point_cloud_cell_id}
      AND name = '{name_reflection_point}'
),
timestamp_sec AS (
    SELECT
      row_number() OVER () AS index,
      timestamp_sec AS value
    FROM sensordb.point_cloud_cell_data,
         LATERAL unnest(val_int8_array) AS timestamp_sec
    WHERE point_cloud_cell_data.point_cloud_cell_id = {point_cloud_cell_id}
      AND name = '{name_timestamp_second}'
),
timestamp_nanosec AS (
    SELECT
      row_number() OVER () AS index,
      timestamp_nanosec AS value
    FROM sensordb.point_cloud_cell_data,
         LATERAL unnest(val_int4_array) AS timestamp_nanosec
    WHERE point_cloud_cell_data.point_cloud_cell_id = {point_cloud_cell_id}
      AND name = '{name_timestamp_nano_second}'
),
interpolated_data AS (
    SELECT
        {point_cloud_cell_id} AS point_cloud_cell_id,
        reflection_point.point_index AS point_index,
        ip.interpolated_position,
        ip.interpolated_orientation
    FROM reflection_point
    LEFT JOIN timestamp_sec ON reflection_point.point_index = timestamp_sec.index
    LEFT JOIN timestamp_nanosec ON reflection_point.point_index = timestamp_nanosec.index
    CROSS JOIN LATERAL sensordb_pkg.interpolate_trajectory_pose({trajectory_id}, timestamp_sec.value, timestamp_nanosec.value) AS ip
),
aggregated_data AS (
    SELECT
        ST_Collect(interpolated_position ORDER BY point_index) as positions,
        ARRAY_AGG(interpolated_orientation ORDER BY point_index) as orientations
    FROM interpolated_data
)
SELECT
    {point_cloud_cell_id},
    {},
    1,
    '{name_sensor_position}',
    positions,
    NULL
FROM aggregated_data
UNION ALL
SELECT
    {point_cloud_cell_id},
    {},
    1,
    '{name_sensor_orientation}',
    NULL,
    orientations
FROM aggregated_data;", DataType::GeometryMultiPoint, DataType::QuaternionArray);
    let _a = sqlx::query(&query).fetch_all(pool).await?;

    Ok(())
}

pub async fn compute_beams(
    pool: &Pool<Postgres>,
    point_cloud_cell_id: PointCloudCellId,
    reflection_uncertainty_line_length: f32,
) -> Result<(), Error> {
    let name_reflection_point = PointCloudCellDataName::ReflectionPoint.as_ref();
    let name_sensor_position = PointCloudCellDataName::SensorPosition.as_ref();
    let name_sensor_orientation = PointCloudCellDataName::SensorOrientation.as_ref();
    let name_beam_line = PointCloudCellDataName::BeamLine.as_ref();
    let name_beam_direction = PointCloudCellDataName::BeamDirection.as_ref();
    let name_spherical_azimuth = PointCloudCellDataName::SphericalAzimuth.as_ref();
    let name_spherical_elevation = PointCloudCellDataName::SphericalElevation.as_ref();
    let name_spherical_range = PointCloudCellDataName::SphericalRange.as_ref();
    let name_reflection_uncertainty_line =
        PointCloudCellDataName::ReflectionUncertaintyLine.as_ref();

    let query = format!(
        "INSERT INTO sensordb.point_cloud_cell_data (
    point_cloud_cell_id,
    datatype_id,
    namespace_id,
    name,
    val_geometry_multi_line_string
)
WITH st AS (
    SELECT
      (dumped).path[1] AS point_index,
      (dumped).geom
    FROM sensordb.point_cloud_cell_data,
         LATERAL ST_Dump(val_geometry_multi_point) AS dumped
    WHERE point_cloud_cell_id = {point_cloud_cell_id}
      AND name = '{name_sensor_position}'
),
rp AS (
    SELECT
      (dumped).path[1] AS point_index,
      (dumped).geom
    FROM sensordb.point_cloud_cell_data,
         LATERAL ST_Dump(val_geometry_multi_point) AS dumped
    WHERE point_cloud_cell_id = {point_cloud_cell_id}
      AND name = '{name_reflection_point}'
)
SELECT
    {point_cloud_cell_id} AS point_cloud_cell_id,
    {} AS datatype_id,
    1 AS namespace_id,
    '{name_beam_line}' AS name,
    ST_Collect(ST_MakeLine(st.geom, rp.geom)) AS val_geometry_multi_line_string
FROM st
JOIN rp ON st.point_index = rp.point_index;",
        DataType::GeometryMultiLineString
    );
    let _a = sqlx::query(&query).fetch_all(pool).await?;

    let query = format!(
        "INSERT INTO sensordb.point_cloud_cell_data (
    point_cloud_cell_id,
    datatype_id,
    namespace_id,
    name,
    val_geometry_multi_point
)
WITH st AS (
    SELECT
      (dumped).path[1] AS point_index,
      ST_X((dumped).geom) AS x,
      ST_Y((dumped).geom) AS y,
      ST_Z((dumped).geom) AS z
    FROM sensordb.point_cloud_cell_data,
         LATERAL ST_Dump(val_geometry_multi_point) AS dumped
    WHERE point_cloud_cell_id = {point_cloud_cell_id}
      AND name = '{name_sensor_position}'
),
rp AS (
    SELECT
      (dumped).path[1] AS point_index,
      ST_X((dumped).geom) AS x,
      ST_Y((dumped).geom) AS y,
      ST_Z((dumped).geom) AS z
    FROM sensordb.point_cloud_cell_data,
         LATERAL ST_Dump(val_geometry_multi_point) AS dumped
    WHERE point_cloud_cell_id = {point_cloud_cell_id}
      AND name = '{name_reflection_point}'
)
SELECT
    {point_cloud_cell_id} AS point_cloud_cell_id,
    {} AS datatype_id,
    1 AS namespace_id,
    '{name_beam_direction}' AS name,
    ST_Collect(ST_MakePoint(rp.x-st.x, rp.y-st.y, rp.z-st.z)) AS val_geometry_multi_point
FROM st
JOIN rp ON st.point_index = rp.point_index;",
        DataType::GeometryMultiPoint
    );
    let _a = sqlx::query(&query).fetch_all(pool).await?;

    let query = format!(
        "INSERT INTO sensordb.point_cloud_cell_data (
    point_cloud_cell_id,
    datatype_id,
    namespace_id,
    name,
    val_float8_array
)
WITH bd AS (
    SELECT
      (dumped).path[1] AS point_index,
      (dumped).geom AS geom
    FROM sensordb.point_cloud_cell_data,
         LATERAL ST_Dump(val_geometry_multi_point) AS dumped
    WHERE point_cloud_cell_id = {point_cloud_cell_id}
      AND name = '{name_beam_direction}'
),
sr AS (
    SELECT
      row_number() OVER () AS quaternion_index,
      quaternion_value.x AS sensor_orientation_x,
      quaternion_value.y AS sensor_orientation_y,
      quaternion_value.z AS sensor_orientation_z,
      quaternion_value.w AS sensor_orientation_w
    FROM sensordb.point_cloud_cell_data,
         LATERAL unnest(val_quaternion_array) AS quaternion_value
    WHERE point_cloud_cell_id = {point_cloud_cell_id}
      AND name = '{name_sensor_orientation}'
),
srr AS (
    SELECT (sensordb_pkg.ST_3DCartesianToSpherical(sensordb_pkg.ST_ApplyUnitQuaternionRotation(bd.geom, -sensor_orientation_x, -sensor_orientation_y, -sensor_orientation_z, sensor_orientation_w))).*
    FROM bd
    JOIN sr ON bd.point_index = sr.quaternion_index
),
aggregated AS (
    SELECT
        array_agg(spherical_azimuth) AS azimuth_array,
        array_agg(spherical_elevation) AS elevation_array,
        array_agg(spherical_range) AS range_array
    FROM srr
)
SELECT
    {point_cloud_cell_id} AS point_cloud_cell_id,
    {} AS datatype_id,
    1 AS namespace_id,
    component_name AS name,
    component_value AS val_float8_array
FROM aggregated
CROSS JOIN LATERAL (
    VALUES
        ('{name_spherical_azimuth}', azimuth_array),
        ('{name_spherical_elevation}', elevation_array),
        ('{name_spherical_range}', range_array)
) AS components(component_name, component_value);",
        DataType::Float8Array
    );
    let _a = sqlx::query(&query).fetch_all(pool).await?;

    let query = format!(
        "INSERT INTO sensordb.point_cloud_cell_data (
    point_cloud_cell_id,
    datatype_id,
    namespace_id,
    name,
    val_geometry_multi_line_string
)
WITH b AS (
    SELECT
      (dumped).path[1] AS index,
      (dumped).geom AS line,
      ST_LineInterpolatePoint((dumped).geom, 0.5) AS midpoint,
      ST_3DLength((dumped).geom) as length
    FROM sensordb.point_cloud_cell_data,
         LATERAL ST_Dump(val_geometry_multi_line_string) AS dumped
    WHERE point_cloud_cell_id = {point_cloud_cell_id}
      AND name = '{name_beam_line}'
),
rp AS (
    SELECT
      (dumped).path[1] AS index,
      (dumped).geom AS geom
    FROM sensordb.point_cloud_cell_data,
         LATERAL ST_Dump(val_geometry_multi_point) AS dumped
    WHERE point_cloud_cell_id = {point_cloud_cell_id}
      AND name = '{name_reflection_point}'
)
SELECT
    {point_cloud_cell_id} AS point_cloud_cell_id,
    {} AS datatype_id,
    1 AS namespace_id,
    '{name_reflection_uncertainty_line}' AS name,
    ST_COLLECT(ST_Translate(
       ST_Scale(
                   ST_Translate(b.line, -ST_X(b.midpoint), -ST_Y(b.midpoint), -ST_Z(b.midpoint)),
                   {reflection_uncertainty_line_length}/b.length, {reflection_uncertainty_line_length}/b.length, {reflection_uncertainty_line_length}/b.length),
           ST_X(rp.geom), ST_Y(rp.geom), ST_Z(rp.geom))) as val_geometry_multi_line_string
FROM b
JOIN rp ON rp.index = b.index;",
        DataType::GeometryMultiLineString
    );
    let _a = sqlx::query(&query).fetch_all(pool).await?;

    Ok(())
}
