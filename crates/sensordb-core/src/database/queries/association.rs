use crate::Error;
use crate::database::datatype::PointCloudCellId;
use sqlx::{Pool, Postgres};

pub async fn explode_feature_geometry_data(pool: &Pool<Postgres>) -> Result<(), Error> {
    let query = "INSERT INTO sensordb.feature_geometry_data (geometry_data_id, feature_id, geometry, normal_vector, centroid)
SELECT
    id,
    feature_id,
    geometry,
    sensordb_pkg.ST_3DNormalVector(geometry) as normal_vector,
    sensordb_pkg.ST_3DPolygonCentroid(geometry) as centroid
FROM
    (SELECT
         id,
         feature_id,
         (ST_Dump(geometry_data.geometry)).geom::geometry(PolygonZ) as geometry
    FROM geometry_data
    WHERE
        ST_GeometryType(geometry_data.geometry) = 'ST_PolyhedralSurface' OR
        ST_GeometryType(geometry_data.geometry) = 'ST_MultiPolygon'
    ) as t;".to_string();

    let _a = sqlx::query(&query).fetch_all(pool).await?;

    Ok(())
}

pub async fn associate(
    pool: &Pool<Postgres>,
    point_cloud_cell_id: PointCloudCellId,
    reflection_uncertainty_point_buffer: f32,
    reflection_uncertainty_line_buffer: f32,
    max_reflection_uncertainty_line_intersection_parameter: f32,
) -> Result<(), Error> {
    let point_cloud_cell_id: i32 = point_cloud_cell_id.into();
    sqlx::query!(
        r#"
        SELECT sensordb_pkg.AssociatePointCloudCellToFeature($1, $2, $3, $4);
        "#,
        point_cloud_cell_id,
        reflection_uncertainty_point_buffer,
        reflection_uncertainty_line_buffer,
        max_reflection_uncertainty_line_intersection_parameter,
    )
    .fetch_optional(pool)
    .await?;

    Ok(())
}
