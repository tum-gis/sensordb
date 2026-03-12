use crate::Error;
use crate::database::datatype::{DataType, Namespace, PointCloudCellDataName, PointCloudCellId};
use crate::database::tables::PointCloudAttributeContext;
use crate::point_cloud_extensions::ExtendedPointDataColumnType;
use epoint::PointDataColumnType;
use polars::frame::DataFrame;
use polars::prelude::{Column, NamedFrom};
use sqlx::{Pool, Postgres};

pub async fn export_point_cloud_cell(
    pool: &Pool<Postgres>,
    point_cloud_cell_id: PointCloudCellId,
    point_cloud_attribute_context: &PointCloudAttributeContext,
) -> Result<DataFrame, Error> {
    let mut dataframe_columns: Vec<Column> = Vec::new();

    dataframe_columns.extend(
        retrieve_point_cloud_cell_data_geometry_multi_point(
            pool,
            point_cloud_cell_id,
            PointCloudCellDataName::ReflectionPoint.as_ref(),
            PointDataColumnType::X.as_str(),
            PointDataColumnType::Y.as_str(),
            PointDataColumnType::Z.as_str(),
        )
        .await?,
    );

    if point_cloud_attribute_context.contains_cell_data_name(PointCloudCellDataName::Id) {
        dataframe_columns.push(
            retrieve_point_cloud_cell_data_int8_array(
                pool,
                point_cloud_cell_id,
                PointCloudCellDataName::Id.as_ref(),
                PointDataColumnType::Id.as_str(),
                false,
            )
            .await?
            .cast(&PointDataColumnType::Id.data_frame_data_type())?,
        );
    }

    if point_cloud_attribute_context.contains_cell_data_name(PointCloudCellDataName::Intensity) {
        dataframe_columns.push(
            retrieve_point_cloud_cell_data_float4_array(
                pool,
                point_cloud_cell_id,
                PointCloudCellDataName::Intensity.as_ref(),
                PointDataColumnType::Intensity.as_str(),
                false,
            )
            .await?,
        );
    }

    if point_cloud_attribute_context
        .contains_cell_data_name(PointCloudCellDataName::TimestampSecond)
    {
        dataframe_columns.push(
            retrieve_point_cloud_cell_data_int8_array(
                pool,
                point_cloud_cell_id,
                PointCloudCellDataName::TimestampSecond.as_ref(),
                PointDataColumnType::TimestampSecond.as_str(),
                false,
            )
            .await?,
        );
    }

    if point_cloud_attribute_context
        .contains_cell_data_name(PointCloudCellDataName::TimestampNanoSecond)
    {
        dataframe_columns.push(
            retrieve_point_cloud_cell_data_int4_array(
                pool,
                point_cloud_cell_id,
                PointCloudCellDataName::TimestampNanoSecond.as_ref(),
                PointDataColumnType::TimestampNanoSecond.as_str(),
                false,
            )
            .await?
            .cast(&PointDataColumnType::TimestampNanoSecond.data_frame_data_type())?,
        );
    }

    if point_cloud_attribute_context.contains_cell_data_name(PointCloudCellDataName::SensorPosition)
    {
        dataframe_columns.extend(
            retrieve_point_cloud_cell_data_geometry_multi_point(
                pool,
                point_cloud_cell_id,
                PointCloudCellDataName::SensorPosition.as_ref(),
                PointDataColumnType::SensorTranslationX.as_str(),
                PointDataColumnType::SensorTranslationY.as_str(),
                PointDataColumnType::SensorTranslationZ.as_str(),
            )
            .await?,
        );
    }

    if point_cloud_attribute_context
        .contains_cell_data_name(PointCloudCellDataName::SphericalAzimuth)
    {
        dataframe_columns.push(
            retrieve_point_cloud_cell_data_float8_array(
                pool,
                point_cloud_cell_id,
                PointCloudCellDataName::SphericalAzimuth.as_ref(),
                PointDataColumnType::SphericalAzimuth.as_str(),
                false,
            )
            .await?,
        );
    }

    if point_cloud_attribute_context
        .contains_cell_data_name(PointCloudCellDataName::SphericalElevation)
    {
        dataframe_columns.push(
            retrieve_point_cloud_cell_data_float8_array(
                pool,
                point_cloud_cell_id,
                PointCloudCellDataName::SphericalElevation.as_ref(),
                PointDataColumnType::SphericalElevation.as_str(),
                false,
            )
            .await?,
        );
    }

    if point_cloud_attribute_context.contains_cell_data_name(PointCloudCellDataName::SphericalRange)
    {
        dataframe_columns.push(
            retrieve_point_cloud_cell_data_float8_array(
                pool,
                point_cloud_cell_id,
                PointCloudCellDataName::SphericalRange.as_ref(),
                PointDataColumnType::SphericalRange.as_str(),
                false,
            )
            .await?,
        );
    }

    if point_cloud_attribute_context.contains_cell_data_name(PointCloudCellDataName::PointSourceId)
    {
        dataframe_columns.push(
            retrieve_point_cloud_cell_data_int4_array(
                pool,
                point_cloud_cell_id,
                PointCloudCellDataName::PointSourceId.as_ref(),
                PointDataColumnType::PointSourceId.as_str(),
                false,
            )
            .await?
            .cast(&PointDataColumnType::PointSourceId.data_frame_data_type())?,
        );
    }

    if point_cloud_attribute_context
        .contains_cell_data_name(PointCloudCellDataName::ReflectionPointSurfaceDistance)
    {
        dataframe_columns.push(
            retrieve_point_cloud_cell_data_float4_array(
                pool,
                point_cloud_cell_id,
                PointCloudCellDataName::ReflectionPointSurfaceDistance.as_ref(),
                ExtendedPointDataColumnType::ReflectionPointSurfaceDistance.as_str(),
                true,
            )
            .await?,
        );
    }

    if point_cloud_attribute_context
        .contains_cell_data_name(PointCloudCellDataName::BeamLineSurfaceDistance)
    {
        dataframe_columns.push(
            retrieve_point_cloud_cell_data_float4_array(
                pool,
                point_cloud_cell_id,
                PointCloudCellDataName::BeamLineSurfaceDistance.as_ref(),
                ExtendedPointDataColumnType::BeamLineSurfaceDistance.as_str(),
                true,
            )
            .await?,
        );
    }

    if point_cloud_attribute_context
        .contains_cell_data_name(PointCloudCellDataName::ReflectionLinePlaneIntersectionParameter)
    {
        dataframe_columns.push(
            retrieve_point_cloud_cell_data_float4_array(
                pool,
                point_cloud_cell_id,
                PointCloudCellDataName::ReflectionLinePlaneIntersectionParameter.as_ref(),
                ExtendedPointDataColumnType::ReflectionLinePlaneIntersectionParameter.as_str(),
                true,
            )
            .await?,
        );
    }

    if point_cloud_attribute_context
        .contains_cell_data_name(PointCloudCellDataName::SurfaceZenithAngle)
    {
        dataframe_columns.push(
            retrieve_point_cloud_cell_data_float4_array(
                pool,
                point_cloud_cell_id,
                PointCloudCellDataName::SurfaceZenithAngle.as_ref(),
                ExtendedPointDataColumnType::SurfaceZenithAngle.as_str(),
                true,
            )
            .await?,
        );
    }

    if point_cloud_attribute_context
        .contains_cell_data_name(PointCloudCellDataName::SurfaceAzimuthAngle)
    {
        dataframe_columns.push(
            retrieve_point_cloud_cell_data_float4_array(
                pool,
                point_cloud_cell_id,
                PointCloudCellDataName::SurfaceAzimuthAngle.as_ref(),
                ExtendedPointDataColumnType::SurfaceAzimuthAngle.as_str(),
                true,
            )
            .await?,
        );
    }

    if point_cloud_attribute_context
        .contains_cell_data_name(PointCloudCellDataName::FeatureGeometryId)
    {
        dataframe_columns.extend(
            retrieve_associated_feature_information(
                pool,
                point_cloud_cell_id,
                PointCloudCellDataName::FeatureGeometryId.as_ref(),
            )
            .await?,
        );
    }

    let data_frame = DataFrame::new(dataframe_columns)?;
    Ok(data_frame)
}

async fn retrieve_point_cloud_cell_data_geometry_multi_point(
    pool: &Pool<Postgres>,
    database_cell_id: PointCloudCellId,
    database_cell_name: &str,
    column_x_name: &str,
    column_y_name: &str,
    column_z_name: &str,
) -> Result<[Column; 3], Error> {
    #[derive(sqlx::FromRow)]
    struct Point3D {
        x: f64,
        y: f64,
        z: f64,
    }

    let retrieved_points: Vec<Point3D> = sqlx::query_as(
        "SELECT
    ST_X(geom) AS x,
    ST_Y(geom) AS y,
    ST_Z(geom) AS z
FROM sensordb.point_cloud_cell_data
CROSS JOIN LATERAL ST_DumpPoints(val_geometry_multi_point) AS geom
WHERE point_cloud_cell_id = $1
   AND name = $2
   AND datatype_id = $3
   AND namespace_id = $4
   AND val_geometry_multi_point IS NOT NULL;",
    )
    .bind(database_cell_id)
    .bind(database_cell_name)
    .bind(DataType::GeometryMultiPoint)
    .bind(Namespace::Core)
    .fetch_all(pool)
    .await?;

    let x_column = Column::new(
        column_x_name.into(),
        retrieved_points.iter().map(|p| p.x).collect::<Vec<f64>>(),
    );
    let y_column = Column::new(
        column_y_name.into(),
        retrieved_points.iter().map(|p| p.y).collect::<Vec<f64>>(),
    );
    let z_column = Column::new(
        column_z_name.into(),
        retrieved_points.iter().map(|p| p.z).collect::<Vec<f64>>(),
    );

    Ok((x_column, y_column, z_column).into())
}

async fn retrieve_point_cloud_cell_data_int4_array(
    pool: &Pool<Postgres>,
    database_cell_id: PointCloudCellId,
    database_cell_name: &str,
    column_name: &str,
    allow_none_values: bool,
) -> Result<Column, Error> {
    let (retrieved_values,): (Vec<Option<i32>>,) = sqlx::query_as(
        "SELECT val_int4_array
         FROM sensordb.point_cloud_cell_data
         WHERE point_cloud_cell_id = $1
           AND name = $2
           AND datatype_id = $3
           AND namespace_id = $4
           AND val_int4_array IS NOT NULL;",
    )
    .bind(database_cell_id)
    .bind(database_cell_name)
    .bind(DataType::Int4Array)
    .bind(Namespace::Core)
    .fetch_one(pool)
    .await?;

    let column = if allow_none_values {
        Column::new(column_name.into(), retrieved_values)
    } else {
        Column::new(
            column_name.into(),
            retrieved_values
                .iter()
                .map(|x| x.expect("must not be none"))
                .collect::<Vec<i32>>(),
        )
    };

    Ok(column)
}

async fn retrieve_point_cloud_cell_data_int8_array(
    pool: &Pool<Postgres>,
    database_cell_id: PointCloudCellId,
    database_cell_name: &str,
    column_name: &str,
    allow_none_values: bool,
) -> Result<Column, Error> {
    let (retrieved_values,): (Vec<Option<i64>>,) = sqlx::query_as(
        "SELECT val_int8_array
         FROM sensordb.point_cloud_cell_data
         WHERE point_cloud_cell_id = $1
           AND name = $2
           AND datatype_id = $3
           AND namespace_id = $4
           AND val_int8_array IS NOT NULL;",
    )
    .bind(database_cell_id)
    .bind(database_cell_name)
    .bind(DataType::Int8Array)
    .bind(Namespace::Core)
    .fetch_one(pool)
    .await?;

    let column = if allow_none_values {
        Column::new(column_name.into(), retrieved_values)
    } else {
        Column::new(
            column_name.into(),
            retrieved_values
                .iter()
                .map(|x| x.expect("must not be none"))
                .collect::<Vec<i64>>(),
        )
    };

    Ok(column)
}

async fn retrieve_point_cloud_cell_data_float4_array(
    pool: &Pool<Postgres>,
    database_cell_id: PointCloudCellId,
    database_cell_name: &str,
    column_name: &str,
    allow_none_values: bool,
) -> Result<Column, Error> {
    let (retrieved_values,): (Vec<Option<f32>>,) = sqlx::query_as(
        "SELECT val_float4_array
         FROM sensordb.point_cloud_cell_data
         WHERE point_cloud_cell_id = $1
           AND name = $2
           AND datatype_id = $3
           AND namespace_id = $4
           AND val_float4_array IS NOT NULL;",
    )
    .bind(database_cell_id)
    .bind(database_cell_name)
    .bind(DataType::Float4Array)
    .bind(Namespace::Core)
    .fetch_one(pool)
    .await?;

    let column = if allow_none_values {
        Column::new(column_name.into(), retrieved_values)
    } else {
        Column::new(
            column_name.into(),
            retrieved_values
                .iter()
                .map(|x| x.expect("must not be none"))
                .collect::<Vec<f32>>(),
        )
    };

    Ok(column)
}

async fn retrieve_point_cloud_cell_data_float8_array(
    pool: &Pool<Postgres>,
    database_cell_id: PointCloudCellId,
    database_cell_name: &str,
    column_name: &str,
    allow_none_values: bool,
) -> Result<Column, Error> {
    let (retrieved_values,): (Vec<Option<f64>>,) = sqlx::query_as(
        "SELECT val_float8_array
         FROM sensordb.point_cloud_cell_data
         WHERE point_cloud_cell_id = $1
           AND name = $2
           AND datatype_id = $3
           AND namespace_id = $4
           AND val_float8_array IS NOT NULL;",
    )
    .bind(database_cell_id)
    .bind(database_cell_name)
    .bind(DataType::Float8Array)
    .bind(Namespace::Core)
    .fetch_one(pool)
    .await?;

    let column = if allow_none_values {
        Column::new(column_name.into(), retrieved_values)
    } else {
        Column::new(
            column_name.into(),
            retrieved_values
                .iter()
                .map(|x| x.expect("must not be none"))
                .collect::<Vec<f64>>(),
        )
    };

    Ok(column)
}

async fn retrieve_associated_feature_information(
    pool: &Pool<Postgres>,
    database_cell_id: PointCloudCellId,
    database_cell_name: &str,
) -> Result<[Column; 3], Error> {
    #[derive(sqlx::FromRow)]
    struct FeatureInfo {
        point_index: i64,
        feature_object_id: Option<String>,
        feature_object_name: Option<String>,
        feature_class_name: Option<String>,
    }

    let retrieved_feature_infos: Vec<FeatureInfo> = sqlx::query_as(
        "SELECT
    point_index,
    --feature_geometry_data_id,
    --feature_geometry_data.feature_id,
    feature.objectid AS feature_object_id,
    p.val_string AS feature_object_name,
    objectclass.classname AS feature_class_name
FROM (SELECT row_number() OVER ()     AS point_index,
             feature_geometry_data_id AS feature_geometry_data_id
      FROM sensordb.point_cloud_cell_data,
           LATERAL unnest(val_geometry_data_id_array) AS feature_geometry_data_id
      WHERE point_cloud_cell_data.point_cloud_cell_id = $1
        AND name = $2
        AND datatype_id = $3
        AND namespace_id = $4) as pc
LEFT JOIN sensordb.feature_geometry_data ON pc.feature_geometry_data_id = feature_geometry_data.id
LEFT JOIN citydb.feature ON feature_geometry_data.feature_id = citydb.feature.id
LEFT JOIN citydb.objectclass ON citydb.feature.objectclass_id = citydb.objectclass.id
LEFT JOIN (
    SELECT *
    FROM citydb.property
    WHERE name = 'name' AND datatype_id = 14 AND namespace_id = 1
) p ON p.feature_id = citydb.feature.id;
",
    )
    .bind(database_cell_id)
    .bind(database_cell_name)
    .bind(DataType::GeometryReferenceArray)
    .bind(Namespace::Core)
    .fetch_all(pool)
    .await?;

    let feature_object_id_column = Column::new(
        ExtendedPointDataColumnType::FeatureObjectId.into(),
        retrieved_feature_infos
            .iter()
            .map(|p| p.feature_object_id.clone())
            .collect::<Vec<Option<String>>>(),
    );
    let feature_object_name_column = Column::new(
        ExtendedPointDataColumnType::FeatureObjectName.into(),
        retrieved_feature_infos
            .iter()
            .map(|p| p.feature_object_name.clone())
            .collect::<Vec<Option<String>>>(),
    );
    let feature_class_name_column = Column::new(
        ExtendedPointDataColumnType::FeatureClassName.into(),
        retrieved_feature_infos
            .into_iter()
            .map(|p| p.feature_class_name)
            .collect::<Vec<Option<String>>>(),
    );

    Ok((
        feature_object_id_column,
        feature_object_name_column,
        feature_class_name_column,
    )
        .into())
}

/*
pub async fn export_point_cloud_time_slice(
    pool: &Pool<Postgres>,
    campaign_id: CampaignId,
    start_date_time: DateTime<Utc>,
    end_date_time: DateTime<Utc>,
) -> Result<epoint::PointCloud, Error> {
    let campaign_id: i32 = campaign_id.into();
    let start_timestamp_sec = start_date_time.timestamp() as i32;
    let start_timestamp_nanosec = start_date_time.nanosecond() as i32;
    let end_timestamp_sec = end_date_time.timestamp() as i32;
    let end_timestamp_nanosec = end_date_time.nanosecond() as i32;

    let database_points: Vec<PointCloudEnrichedEntry> = sqlx::query_as!(
        PointCloudEnrichedEntry,
        r#"
        SELECT *
        FROM sensordb.lidar_beam_enriched
        WHERE campaign_id = $1
        AND (
            (timestamp_sec = $2 AND timestamp_nanosec >= $3)
            OR (timestamp_sec > $2 AND timestamp_sec < $4)
            OR (timestamp_sec = $4 AND timestamp_nanosec < $5)
        )
        "#,
        campaign_id,
        start_timestamp_sec,
        start_timestamp_nanosec,
        end_timestamp_sec,
        end_timestamp_nanosec
    )
    .fetch_all(pool)
    .await?;

    //let number_of_points = database_points.len();
    //info!("Number of points in time slice {start_date_time}-{end_date_time}: {number_of_points}");

    let point_cloud = derive_point_cloud(database_points)?;
    Ok(point_cloud)
}*/

pub async fn export_point_cloud_for_associated_feature(
    _pool: &Pool<Postgres>,
    feature_object_id: String,
) -> Result<epoint::PointCloud, Error> {
    todo!("export point cloud for {}", feature_object_id);

    // let point_cloud = derive_point_cloud(database_points)?;
    // Ok(point_cloud)
}
