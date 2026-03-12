use crate::database::datatype::{
    DataType, Namespace, PointCloudCellDataName, PointCloudCellId, PointCloudId,
};
use crate::database::tables::PointCloudCellEntry;
use crate::error::Error;
use ecoord::AxisAlignedBoundingCube;
use ecoord::octree::OctantIndex;
use epoint::PointCloud;
use itertools::Itertools;
use nalgebra::{Point3, UnitQuaternion};
use sqlx::{Pool, Postgres};

pub async fn insert_point_cloud_cell(
    pool: &Pool<Postgres>,
    point_cloud_id: PointCloudId,
    octant_index: OctantIndex,
    octant_bounding_cube: AxisAlignedBoundingCube,
    point_cloud: &PointCloud,
    metadata_only: bool,
) -> Result<(), Error> {
    let point_cloud_cell_entry = register_point_cloud_cell(
        pool,
        point_cloud_id,
        octant_index,
        octant_bounding_cube,
        point_cloud,
    )
    .await?;

    insert_point_cell_metadata_values(pool, point_cloud_cell_entry.id, point_cloud).await?;
    if !metadata_only {
        insert_point_cell_data_values(pool, point_cloud_cell_entry.id, point_cloud).await?;
    }

    Ok(())
}

async fn register_point_cloud_cell(
    pool: &Pool<Postgres>,
    point_cloud_id: PointCloudId,
    octant_index: OctantIndex,
    octant_bounding_cube: AxisAlignedBoundingCube,
    point_cloud: &epoint::PointCloud,
) -> Result<PointCloudCellEntry, Error> {
    let point_envelope_min = point_cloud.point_data.get_local_min();
    let point_envelope_max = point_cloud.point_data.get_local_max();
    let point_envelope_statement = format!(
        "sensordb_pkg.ST_3DMakeEnvelope(ST_MakePoint({}, {}, {}), ST_MakePoint({}, {}, {}), (SELECT srid FROM database_srs LIMIT 1))",
        point_envelope_min.x,
        point_envelope_min.y,
        point_envelope_min.z,
        point_envelope_max.x,
        point_envelope_max.y,
        point_envelope_max.z
    );
    let octant_bounding_cube_lower_bound = octant_bounding_cube.get_lower_bound();
    let octant_bounding_cube_upper_bound = octant_bounding_cube.get_upper_bound();
    let cell_envelope_statement = format!(
        "sensordb_pkg.ST_3DMakeEnvelope(ST_MakePoint({}, {}, {}), ST_MakePoint({}, {}, {}), (SELECT srid FROM database_srs LIMIT 1))",
        octant_bounding_cube_lower_bound.x,
        octant_bounding_cube_lower_bound.y,
        octant_bounding_cube_lower_bound.z,
        octant_bounding_cube_upper_bound.x,
        octant_bounding_cube_upper_bound.y,
        octant_bounding_cube_upper_bound.z
    );

    let start_date_time = point_cloud.point_data.get_timestamp_min()?;
    let end_date_time = point_cloud.point_data.get_timestamp_max()?;

    let new_entry = sqlx::query_as::<_, PointCloudCellEntry>(&format!(
    "INSERT INTO sensordb.point_cloud_cell (point_cloud_id,level,x,y,z,start_date,end_date,cell_envelope,point_envelope,point_count)
        SELECT $1,$2,$3,$4,$5,$6,$7,{cell_envelope_statement},{point_envelope_statement},$8
        RETURNING id,point_cloud_id,level,x,y,z"
))
    .bind(point_cloud_id)
    .bind(octant_index.level as i32)
    .bind(octant_index.x as i64)
    .bind(octant_index.y as i64)
    .bind(octant_index.z as i64)
    .bind(start_date_time)
    .bind(end_date_time)
    .bind(point_cloud.point_data.height() as i32)
    .fetch_one(pool)
    .await?;

    Ok(new_entry)
}

async fn insert_point_cell_data_values(
    pool: &Pool<Postgres>,
    point_cloud_cell_id: PointCloudCellId,
    point_cloud: &epoint::PointCloud,
) -> Result<(), Error> {
    insert_point_cloud_cell_data_geometry_multi_point(
        pool,
        point_cloud_cell_id,
        PointCloudCellDataName::ReflectionPoint.as_ref(),
        point_cloud.point_data.get_all_points(),
    )
    .await?;
    if let Ok(sensor_position_values) = point_cloud.point_data.get_all_sensor_translations() {
        insert_point_cloud_cell_data_geometry_multi_point(
            pool,
            point_cloud_cell_id,
            PointCloudCellDataName::SensorPosition.as_ref(),
            sensor_position_values,
        )
        .await?;
    }

    insert_point_cloud_cell_data_int8_array(
        pool,
        point_cloud_cell_id,
        PointCloudCellDataName::Id.as_ref(),
        &point_cloud
            .point_data
            .get_id_values()?
            .into_iter()
            .map(|x| x.unwrap() as i64)
            .collect::<Vec<_>>(),
    )
    .await?;

    if let Ok(timestamp_sec_values) = point_cloud.point_data.get_timestamp_sec_values() {
        insert_point_cloud_cell_data_int8_array(
            pool,
            point_cloud_cell_id,
            PointCloudCellDataName::TimestampSecond.as_ref(),
            &timestamp_sec_values
                .into_iter()
                .map(|x| x.unwrap())
                .collect::<Vec<_>>(),
        )
        .await?;
    }

    if let Ok(timestamp_nanosec_values) = point_cloud.point_data.get_timestamp_nanosec_values() {
        insert_point_cloud_cell_data_int4_array(
            pool,
            point_cloud_cell_id,
            PointCloudCellDataName::TimestampNanoSecond.as_ref(),
            &timestamp_nanosec_values
                .into_iter()
                .map(|x| x.unwrap() as i32)
                .collect::<Vec<_>>(),
        )
        .await?;
    }

    if let Ok(intensity_values) = point_cloud.point_data.get_intensity_values() {
        insert_point_cloud_cell_data_float4_array(
            pool,
            point_cloud_cell_id,
            PointCloudCellDataName::Intensity.as_ref(),
            &intensity_values
                .into_iter()
                .map(|x| x.unwrap())
                .collect::<Vec<_>>(),
        )
        .await?;
    }

    if let Ok(sensor_orientation_values) = point_cloud.point_data.get_all_sensor_rotations() {
        insert_point_cloud_cell_data_quaternion_array(
            pool,
            point_cloud_cell_id,
            PointCloudCellDataName::SensorOrientation.as_ref(),
            &sensor_orientation_values.into_iter().collect::<Vec<_>>(),
        )
        .await?;
    }

    if let Ok(point_source_id) = point_cloud.point_data.get_point_source_id_values() {
        insert_point_cloud_cell_data_int4_array(
            pool,
            point_cloud_cell_id,
            PointCloudCellDataName::PointSourceId.as_ref(),
            &point_source_id
                .into_iter()
                .map(|x| x.unwrap() as i32)
                .collect::<Vec<_>>(),
        )
        .await?;
    }

    Ok(())
}

async fn insert_point_cell_metadata_values(
    pool: &Pool<Postgres>,
    point_cloud_cell_id: PointCloudCellId,
    point_cloud: &epoint::PointCloud,
) -> Result<(), Error> {
    let reflection_envelope_min = point_cloud.point_data.get_local_min();
    let reflection_envelope_max = point_cloud.point_data.get_local_max();
    let reflection_envelope_statement = format!(
        "sensordb_pkg.ST_3DMakeEnvelope(ST_MakePoint({}, {}, {}), ST_MakePoint({}, {}, {}), (SELECT srid FROM database_srs LIMIT 1))",
        reflection_envelope_min.x,
        reflection_envelope_min.y,
        reflection_envelope_min.z,
        reflection_envelope_max.x,
        reflection_envelope_max.y,
        reflection_envelope_max.z
    );
    let sql_statement = format!(
        "INSERT INTO sensordb.point_cloud_cell_data
            (point_cloud_cell_id, datatype_id, namespace_id, name, val_geometry_polygon)
         SELECT $1, $2, $3, $4, {reflection_envelope_statement};"
    );
    sqlx::query(&sql_statement)
        .bind(point_cloud_cell_id)
        .bind(DataType::GeometryPolygon)
        .bind(Namespace::Core)
        .bind(PointCloudCellDataName::ReflectionEnvelope.as_ref())
        .execute(pool)
        .await?;

    if point_cloud.contains_sensor_translation() {
        let sensor_position_min = point_cloud.point_data.get_local_sensor_translation_min()?;
        let sensor_position_max = point_cloud.point_data.get_local_sensor_translation_max()?;
        let sensor_position_envelope_statement = format!(
            "sensordb_pkg.ST_3DMakeEnvelope(ST_MakePoint({}, {}, {}), ST_MakePoint({}, {}, {}), (SELECT srid FROM database_srs LIMIT 1))",
            sensor_position_min.x,
            sensor_position_min.y,
            sensor_position_min.z,
            sensor_position_max.x,
            sensor_position_max.y,
            sensor_position_max.z
        );
        let sql_statement = format!(
            "INSERT INTO sensordb.point_cloud_cell_data
            (point_cloud_cell_id, datatype_id, namespace_id, name, val_geometry_polygon)
            SELECT $1, $2, $3, $4, {sensor_position_envelope_statement};"
        );
        sqlx::query(&sql_statement)
            .bind(point_cloud_cell_id)
            .bind(DataType::GeometryPolygon)
            .bind(Namespace::Core)
            .bind(PointCloudCellDataName::SensorPositionEnvelope.as_ref())
            .execute(pool)
            .await?;
    }

    Ok(())
}

async fn insert_point_cloud_cell_data_geometry_multi_point(
    pool: &Pool<Postgres>,
    point_cloud_cell_id: PointCloudCellId,
    field_name: &str,
    points: Vec<Point3<f64>>,
) -> Result<(), Error> {
    let values_sql: String = points
        .into_iter()
        .map(|p| format!("{} {} {}", p.x, p.y, p.z))
        .intersperse(",".into())
        .collect();
    let values_sql = format!("MULTIPOINTZ({})", values_sql);

    let sql_statement = "INSERT INTO sensordb.point_cloud_cell_data
            (point_cloud_cell_id, datatype_id, namespace_id, name, val_geometry_multi_point)
         SELECT $1, $2, $3, $4, ST_GeomFromText($5, (SELECT srid FROM sensordb.database_srs));";

    sqlx::query(sql_statement)
        .bind(point_cloud_cell_id)
        .bind(DataType::GeometryMultiPoint)
        .bind(Namespace::Core)
        .bind(field_name)
        .bind(values_sql)
        .execute(pool)
        .await?;

    Ok(())
}

async fn insert_point_cloud_cell_data_quaternion_array(
    pool: &Pool<Postgres>,
    point_cloud_cell_id: PointCloudCellId,
    field_name: &str,
    values: &Vec<UnitQuaternion<f64>>,
) -> Result<(), Error> {
    let values_sql: String = values
        .iter()
        .map(|q| format!("ROW({},{},{},{})", q.i, q.j, q.k, q.w))
        .intersperse(",".into())
        .collect();
    let values_sql = format!("ARRAY[{values_sql}]::sensordb.quaternion[]");

    let sql_statement = format!(
        "INSERT INTO sensordb.point_cloud_cell_data
            (point_cloud_cell_id, datatype_id, namespace_id, name, val_quaternion_array)
         SELECT $1, $2, $3, $4, {values_sql};"
    );

    sqlx::query(&sql_statement)
        .bind(point_cloud_cell_id)
        .bind(DataType::QuaternionArray)
        .bind(Namespace::Core)
        .bind(field_name)
        //.bind(values_sql)
        .execute(pool)
        .await?;

    Ok(())
}

async fn insert_point_cloud_cell_data_int4_array(
    pool: &Pool<Postgres>,
    point_cloud_cell_id: PointCloudCellId,
    field_name: &str,
    values: &Vec<i32>,
) -> Result<(), Error> {
    let sql_statement = "INSERT INTO sensordb.point_cloud_cell_data
            (point_cloud_cell_id, datatype_id, namespace_id, name, val_int4_array)
         SELECT $1, $2, $3, $4, $5;";

    sqlx::query(sql_statement)
        .bind(point_cloud_cell_id)
        .bind(DataType::Int4Array)
        .bind(Namespace::Core)
        .bind(field_name)
        .bind(values)
        .execute(pool)
        .await?;

    Ok(())
}

async fn insert_point_cloud_cell_data_int8_array(
    pool: &Pool<Postgres>,
    point_cloud_cell_id: PointCloudCellId,
    field_name: &str,
    values: &Vec<i64>,
) -> Result<(), Error> {
    let sql_statement = "INSERT INTO sensordb.point_cloud_cell_data
            (point_cloud_cell_id, datatype_id, namespace_id, name, val_int8_array)
         SELECT $1, $2, $3, $4, $5;";

    sqlx::query(sql_statement)
        .bind(point_cloud_cell_id)
        .bind(DataType::Int8Array)
        .bind(Namespace::Core)
        .bind(field_name)
        .bind(values)
        .execute(pool)
        .await?;

    Ok(())
}

async fn insert_point_cloud_cell_data_float4_array(
    pool: &Pool<Postgres>,
    point_cloud_cell_id: PointCloudCellId,
    field_name: &str,
    values: &Vec<f32>,
) -> Result<(), Error> {
    let sql_statement = "INSERT INTO sensordb.point_cloud_cell_data
            (point_cloud_cell_id, datatype_id, namespace_id, name, val_float4_array)
         SELECT $1, $2, $3, $4, $5;";

    sqlx::query(sql_statement)
        .bind(point_cloud_cell_id)
        .bind(DataType::Float4Array)
        .bind(Namespace::Core)
        .bind(field_name)
        .bind(values)
        .execute(pool)
        .await?;

    Ok(())
}

async fn insert_point_cloud_cell_data_float8_array(
    pool: &Pool<Postgres>,
    point_cloud_cell_id: PointCloudCellId,
    field_name: &str,
    values: &Vec<f64>,
) -> Result<(), Error> {
    let sql_statement = "INSERT INTO sensordb.point_cloud_cell_data
            (point_cloud_cell_id, datatype_id, namespace_id, name, val_float8_array)
         SELECT $1, $2, $3, $4, $5;";

    sqlx::query(sql_statement)
        .bind(point_cloud_cell_id)
        .bind(DataType::Float8Array)
        .bind(Namespace::Core)
        .bind(field_name)
        .bind(values)
        .execute(pool)
        .await?;

    Ok(())
}
