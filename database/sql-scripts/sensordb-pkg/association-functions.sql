CREATE OR REPLACE FUNCTION sensordb_pkg.AssociatePointCloudCellToFeature(
    point_cloud_cell_id INT4,
    reflection_uncertainty_point_buffer FLOAT4,
    reflection_uncertainty_line_buffer FLOAT4,
    max_reflection_uncertainty_line_intersection_parameter FLOAT4
)  RETURNS INT AS
$$
DECLARE
    count INT;
    beam_exist BOOL;
BEGIN
    SELECT
    EXISTS(
        SELECT 1
        FROM sensordb.point_cloud_cell_data
        WHERE point_cloud_cell_data.point_cloud_cell_id = AssociatePointCloudCellToFeature.point_cloud_cell_id AND name = 'BeamLine'
    ) AND
    EXISTS(
        SELECT 1
        FROM sensordb.point_cloud_cell_data
        WHERE point_cloud_cell_data.point_cloud_cell_id = AssociatePointCloudCellToFeature.point_cloud_cell_id AND name = 'ReflectionUncertaintyLine'
    ) AND
    EXISTS(
        SELECT 1
        FROM sensordb.point_cloud_cell_data
        WHERE point_cloud_cell_data.point_cloud_cell_id = AssociatePointCloudCellToFeature.point_cloud_cell_id AND name = 'ReflectionPoint'
    ) AND
    EXISTS(
        SELECT 1
        FROM sensordb.point_cloud_cell_data
        WHERE point_cloud_cell_data.point_cloud_cell_id = AssociatePointCloudCellToFeature.point_cloud_cell_id AND name = 'BeamDirection'
    ) INTO beam_exist;

    IF beam_exist THEN
        count := sensordb_pkg.AssociatePointCloudCellBeamToFeature(
            point_cloud_cell_id,
            reflection_uncertainty_line_buffer,
            max_reflection_uncertainty_line_intersection_parameter
        );
    ELSE
        count := sensordb_pkg.AssociatePointCloudCellPointToFeature(
            point_cloud_cell_id,
            reflection_uncertainty_point_buffer
        );
    END IF;

    RETURN count;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION sensordb_pkg.AssociatePointCloudCellPointToFeature(
    point_cloud_cell_id INT4,
    reflection_uncertainty_point_buffer FLOAT4
)  RETURNS INT AS
$$
DECLARE
    reflection_point_envelope geometry(GeometryZ);
    count INT;
BEGIN
    SELECT Box3D(val_geometry_multi_point)::geometry
    INTO reflection_point_envelope
    FROM sensordb.point_cloud_cell_data
    WHERE point_cloud_cell_data.point_cloud_cell_id = AssociatePointCloudCellPointToFeature.point_cloud_cell_id
      AND name = 'ReflectionPoint';

    CREATE TEMP TABLE temp_feature_geometry_data (
        id INT8 NOT NULL,
        geometry_data_id INT8 NOT NULL,
        feature_id INT8 NOT NULL,
        geometry geometry(PolygonZ) NOT NULL,
        normal_vector geometry(PointZ) NOT NULL,
        centroid geometry(PointZ)
    ) ON COMMIT DROP;
    INSERT INTO temp_feature_geometry_data
    SELECT *
    FROM sensordb.feature_geometry_data
    WHERE feature_geometry_data.normal_vector IS NOT NULL AND ST_3DIntersects(geometry, reflection_point_envelope);
    CREATE INDEX idx_temp_feature_geometry_data_geometry ON temp_feature_geometry_data USING gist(geometry gist_geometry_ops_nd);


    CREATE TEMP TABLE temp_point_cloud_data (
        point_index INT4,
        id INT4,
        reflection_point geometry
    ) ON COMMIT DROP;
    INSERT INTO temp_point_cloud_data
    WITH
    id_data AS (
        SELECT
          row_number() OVER () AS index,
          id_value AS id_value
        FROM sensordb.point_cloud_cell_data,
             LATERAL unnest(val_int8_array) AS id_value
        WHERE point_cloud_cell_data.point_cloud_cell_id = AssociatePointCloudCellPointToFeature.point_cloud_cell_id
          AND name = 'Id'
    ),
    reflection_points AS (
        SELECT
          (dumped).path[1] AS index,
          (dumped).geom AS geom
        FROM sensordb.point_cloud_cell_data,
             LATERAL ST_Dump(val_geometry_multi_point) AS dumped
        WHERE point_cloud_cell_data.point_cloud_cell_id = AssociatePointCloudCellPointToFeature.point_cloud_cell_id
          AND name = 'ReflectionPoint'
    )
    SELECT
        id_data.index AS point_index,
        id_data.id_value AS id,
        reflection_points.geom AS reflection_point
    FROM id_data
    JOIN reflection_points ON id_data.index = reflection_points.index;


    CREATE TEMP TABLE temp_association_candidates (
        id SERIAL PRIMARY KEY,
        point_index INT4,
        feature_geometry_data_id INT8,
        feature_id INT8,
        reflection_point_surface_distance FLOAT4
    ) ON COMMIT DROP;
    INSERT INTO temp_association_candidates (point_index, feature_geometry_data_id, feature_id, reflection_point_surface_distance)
    SELECT
        pc.point_index AS point_index,
        fgd.id AS feature_geometry_data_id,
        fgd.feature_id AS feature_id,
        ST_3DDistance(pc.reflection_point, fgd.geometry) AS reflection_point_surface_distance
    FROM temp_point_cloud_data AS pc
    JOIN temp_feature_geometry_data AS fgd
    ON ST_3DDWithin(fgd.geometry, pc.reflection_point, AssociatePointCloudCellPointToFeature.reflection_uncertainty_point_buffer);

    CREATE TEMP TABLE temp_association_confirmed
    (
        id SERIAL PRIMARY KEY,
        candidate_id INT4 NOT NULL,
        return_number INT4 NOT NULL
    ) ON COMMIT DROP;
    INSERT INTO temp_association_confirmed (candidate_id, return_number)
    SELECT candidate_id, return_number
    FROM (
        SELECT
            id as candidate_id,
            ROW_NUMBER() OVER (
                PARTITION BY ac.point_index
                ORDER BY ac.reflection_point_surface_distance
            ) AS return_number
        FROM temp_association_candidates AS ac
    ) as fc
    WHERE return_number <= 1;


    CREATE TEMP TABLE temp_enriched_point_cloud_data (
        point_index INT4,
        feature_geometry_data_id INT8,
        feature_id INT8,
        reflection_point_surface_distance FLOAT4
    ) ON COMMIT DROP;
    INSERT INTO temp_enriched_point_cloud_data (point_index, feature_geometry_data_id, feature_id, reflection_point_surface_distance)
    SELECT pc.point_index, a.feature_geometry_data_id, a.feature_id, a.reflection_point_surface_distance
    FROM temp_point_cloud_data AS pc
    LEFT JOIN
        (SELECT point_index, feature_geometry_data_id, feature_id, reflection_point_surface_distance
         FROM temp_association_confirmed
         JOIN temp_association_candidates
         ON temp_association_confirmed.candidate_id = temp_association_candidates.id) AS a
    ON a.point_index = pc.point_index;

    -- insertions into point_cloud_cell_data
    INSERT INTO sensordb.point_cloud_cell_data
        (point_cloud_cell_id, datatype_id, namespace_id, name, val_geometry_data_id_array)
    VALUES
        (AssociatePointCloudCellPointToFeature.point_cloud_cell_id, 22, 1, 'FeatureGeometryId', (SELECT ARRAY_AGG(temp_enriched_point_cloud_data.feature_geometry_data_id ORDER BY point_index) FROM temp_enriched_point_cloud_data));

    INSERT INTO sensordb.point_cloud_cell_data
        (point_cloud_cell_id, datatype_id, namespace_id, name, val_float4_array)
    VALUES
        (AssociatePointCloudCellPointToFeature.point_cloud_cell_id, 12, 1, 'ReflectionPointSurfaceDistance', (SELECT ARRAY_AGG(temp_enriched_point_cloud_data.reflection_point_surface_distance ORDER BY point_index) FROM temp_enriched_point_cloud_data));

    count := (SELECT COUNT(*)::INT FROM temp_association_candidates);

    DROP INDEX idx_temp_feature_geometry_data_geometry;
    DROP TABLE temp_point_cloud_data;
    DROP TABLE temp_feature_geometry_data;
    DROP TABLE temp_association_candidates;
    DROP TABLE temp_association_confirmed;
    DROP TABLE temp_enriched_point_cloud_data;

    RETURN count;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION sensordb_pkg.AssociatePointCloudCellBeamToFeature(
    point_cloud_cell_id INT4,
    reflection_uncertainty_line_buffer FLOAT4,
    max_reflection_uncertainty_line_intersection_parameter FLOAT4
)  RETURNS INT AS
$$
DECLARE
    reflection_line_envelope geometry(GeometryZ);
    count INT;
BEGIN
    SELECT Box3D(val_geometry_multi_line_string)::geometry
    INTO reflection_line_envelope
    FROM sensordb.point_cloud_cell_data
    WHERE point_cloud_cell_data.point_cloud_cell_id = AssociatePointCloudCellBeamToFeature.point_cloud_cell_id
      AND name = 'ReflectionUncertaintyLine';
    -- RAISE NOTICE 'box_result: %', ST_AsText(reflection_line_envelope);

    CREATE TEMP TABLE temp_feature_geometry_data (
        id INT8 NOT NULL,
        geometry_data_id INT8 NOT NULL,
        feature_id INT8 NOT NULL,
        geometry geometry(PolygonZ) NOT NULL,
        normal_vector geometry(PointZ) NOT NULL,
        centroid geometry(PointZ)
    ) ON COMMIT DROP;
    INSERT INTO temp_feature_geometry_data
    SELECT *
    FROM sensordb.feature_geometry_data
    WHERE feature_geometry_data.normal_vector IS NOT NULL AND ST_3DIntersects(geometry, reflection_line_envelope);
    CREATE INDEX idx_temp_feature_geometry_data_geometry ON temp_feature_geometry_data USING gist(geometry gist_geometry_ops_nd);
    -- RAISE NOTICE 'temp_feature_geometry_data sample: %', (SELECT json_agg(row_to_json(ac)) FROM (SELECT * FROM temp_feature_geometry_data LIMIT 5) ac);

    CREATE TEMP TABLE temp_point_cloud_data (
        point_index INT4,
        id INT4,
        beam_line geometry,
        reflection_uncertainty_line geometry,
        reflection_point geometry,
        beam_direction geometry
    ) ON COMMIT DROP;
    INSERT INTO temp_point_cloud_data
    WITH
    id_data AS (
        SELECT
          row_number() OVER () AS index,
          id_value AS id_value
        FROM sensordb.point_cloud_cell_data,
             LATERAL unnest(val_int8_array) AS id_value
        WHERE point_cloud_cell_data.point_cloud_cell_id = AssociatePointCloudCellBeamToFeature.point_cloud_cell_id
          AND name = 'Id'
    ),
    beam_lines AS (
        SELECT
          (dumped).path[1] AS index,
          (dumped).geom AS geom
        FROM sensordb.point_cloud_cell_data,
             LATERAL ST_Dump(val_geometry_multi_line_string) AS dumped
        WHERE point_cloud_cell_data.point_cloud_cell_id = AssociatePointCloudCellBeamToFeature.point_cloud_cell_id
          AND name = 'BeamLine'
    ),
    reflection_uncertainty_lines AS (
        SELECT
          (dumped).path[1] AS index,
          (dumped).geom AS geom
        FROM sensordb.point_cloud_cell_data,
             LATERAL ST_Dump(val_geometry_multi_line_string) AS dumped
        WHERE point_cloud_cell_data.point_cloud_cell_id = AssociatePointCloudCellBeamToFeature.point_cloud_cell_id
          AND name = 'ReflectionUncertaintyLine'
    ),
    reflection_points AS (
        SELECT
          (dumped).path[1] AS index,
          (dumped).geom AS geom
        FROM sensordb.point_cloud_cell_data,
             LATERAL ST_Dump(val_geometry_multi_point) AS dumped
        WHERE point_cloud_cell_data.point_cloud_cell_id = AssociatePointCloudCellBeamToFeature.point_cloud_cell_id
          AND name = 'ReflectionPoint'
    ),
    beam_directions AS (
        SELECT
          (dumped).path[1] AS index,
          (dumped).geom AS geom
        FROM sensordb.point_cloud_cell_data,
             LATERAL ST_Dump(val_geometry_multi_point) AS dumped
        WHERE point_cloud_cell_data.point_cloud_cell_id = AssociatePointCloudCellBeamToFeature.point_cloud_cell_id
          AND name = 'BeamDirection'
    )
    SELECT
        id_data.index AS point_index,
        id_data.id_value AS id,
        beam_lines.geom AS beam_line,
        reflection_uncertainty_lines.geom AS reflection_uncertainty_line,
        reflection_points.geom AS reflection_point,
        beam_directions.geom AS beam_direction
    FROM id_data
    JOIN beam_lines ON id_data.index = beam_lines.index
    JOIN reflection_points ON id_data.index = reflection_points.index
    JOIN reflection_uncertainty_lines ON id_data.index = reflection_uncertainty_lines.index
    JOIN beam_directions ON id_data.index = beam_directions.index;

    -- association candidates are all points that have a feature geometry within the buffer of the reflection
    -- uncertainty line
    CREATE TEMP TABLE temp_association_candidates (
        id SERIAL PRIMARY KEY,
        point_index INT4,
        feature_geometry_data_id INT8,
        feature_id INT8,
        reflection_point_surface_distance FLOAT4,
        beam_line_surface_distance FLOAT4,
        surface_zenith_angle FLOAT4,
        surface_azimuth_angle FLOAT4,
        reflection_line_plane_intersection_parameter FLOAT4
    ) ON COMMIT DROP;
    INSERT INTO temp_association_candidates (point_index, feature_geometry_data_id, feature_id, reflection_point_surface_distance, beam_line_surface_distance, surface_zenith_angle, surface_azimuth_angle, reflection_line_plane_intersection_parameter)
    SELECT
        pc.point_index AS point_index,
        fgd.id AS feature_geometry_data_id,
        fgd.feature_id AS feature_id,
        ST_3DDistance(pc.reflection_point, fgd.geometry) AS reflection_point_surface_distance,
        ST_3DDistance(pc.beam_line, fgd.geometry) AS beam_line_surface_distance,
        sensordb_pkg.ST_3DZenithAngle(fgd.geometry, pc.beam_line) AS surface_zenith_angle,
        sensordb_pkg.ST_3DAzimuthAngle(fgd.geometry, pc.beam_line) AS surface_azimuth_angle,
        sensordb_pkg.ST_3DLinePlaneIntersectionParameter(pc.reflection_point, pc.beam_direction, fgd.centroid, fgd.normal_vector) AS reflection_line_plane_intersection_parameter
    FROM temp_point_cloud_data AS pc
    JOIN temp_feature_geometry_data AS fgd
    ON ST_3DDWithin(fgd.geometry, pc.reflection_uncertainty_line, AssociatePointCloudCellBeamToFeature.reflection_uncertainty_line_buffer);

    -- RAISE NOTICE 'association_candidates row count: %', (SELECT COUNT(*) FROM temp_association_candidates);
    -- RAISE NOTICE 'association_candidates sample: %', (SELECT row_to_json(ac) FROM temp_association_candidates ac LIMIT 1);

    CREATE TEMP TABLE temp_association_confirmed
    (
        id SERIAL PRIMARY KEY,
        candidate_id INT4 NOT NULL,
        return_number INT4 NOT NULL
    ) ON COMMIT DROP;
    INSERT INTO temp_association_confirmed (candidate_id, return_number)
    SELECT candidate_id, return_number
    FROM (
     SELECT id as candidate_id, ROW_NUMBER() OVER (
         PARTITION BY ac.point_index
         ORDER BY ac.reflection_line_plane_intersection_parameter, ac.reflection_point_surface_distance
     ) AS return_number
     FROM temp_association_candidates AS ac
     WHERE surface_zenith_angle IS NOT NULL AND
       reflection_line_plane_intersection_parameter IS NOT NULL AND
       ABS(reflection_line_plane_intersection_parameter) <= (AssociatePointCloudCellBeamToFeature.max_reflection_uncertainty_line_intersection_parameter / 2)) as fc
     WHERE return_number <= 1;

    -- RAISE NOTICE 'temp_association_confirmed row count: %', (SELECT COUNT(*) FROM temp_association_confirmed);
    -- RAISE NOTICE 'temp_association_confirmed sample: %', (SELECT row_to_json(ac) FROM temp_association_confirmed ac LIMIT 1);

    CREATE TEMP TABLE temp_enriched_point_cloud_data (
        point_index INT4,
        feature_geometry_data_id INT8,
        feature_id INT8,
        reflection_point_surface_distance FLOAT4,
        beam_line_surface_distance FLOAT4,
        surface_zenith_angle FLOAT4,
        surface_azimuth_angle FLOAT4,
        reflection_line_plane_intersection_parameter FLOAT4
    ) ON COMMIT DROP;
    INSERT INTO temp_enriched_point_cloud_data (point_index, feature_geometry_data_id, feature_id, reflection_point_surface_distance, beam_line_surface_distance, surface_zenith_angle, surface_azimuth_angle, reflection_line_plane_intersection_parameter)
    SELECT pc.point_index, a.feature_geometry_data_id, a.feature_id, a.reflection_point_surface_distance, a.beam_line_surface_distance, a.surface_zenith_angle, a.surface_azimuth_angle, a.reflection_line_plane_intersection_parameter
    FROM temp_point_cloud_data AS pc
    LEFT JOIN
        (SELECT point_index, feature_geometry_data_id, feature_id, reflection_point_surface_distance, beam_line_surface_distance, surface_zenith_angle, surface_azimuth_angle, reflection_line_plane_intersection_parameter
         FROM temp_association_confirmed
         JOIN temp_association_candidates
         ON temp_association_confirmed.candidate_id = temp_association_candidates.id) AS a
    ON a.point_index = pc.point_index;

    -- RAISE NOTICE 'temp_enriched_point_cloud_data row count: %', (SELECT COUNT(*) FROM temp_enriched_point_cloud_data);
    -- RAISE NOTICE 'temp_enriched_point_cloud_data sample: %', (SELECT row_to_json(ac) FROM temp_enriched_point_cloud_data ac LIMIT 1);

    -- insertions into point_cloud_cell_data
    INSERT INTO sensordb.point_cloud_cell_data
        (point_cloud_cell_id, datatype_id, namespace_id, name, val_geometry_data_id_array)
    VALUES
        (AssociatePointCloudCellBeamToFeature.point_cloud_cell_id, 22, 1, 'FeatureGeometryId', (SELECT ARRAY_AGG(temp_enriched_point_cloud_data.feature_geometry_data_id ORDER BY point_index) FROM temp_enriched_point_cloud_data));

    INSERT INTO sensordb.point_cloud_cell_data
        (point_cloud_cell_id, datatype_id, namespace_id, name, val_float4_array)
    VALUES
        (AssociatePointCloudCellBeamToFeature.point_cloud_cell_id, 12, 1, 'ReflectionPointSurfaceDistance', (SELECT ARRAY_AGG(temp_enriched_point_cloud_data.reflection_point_surface_distance ORDER BY point_index) FROM temp_enriched_point_cloud_data));

    INSERT INTO sensordb.point_cloud_cell_data
        (point_cloud_cell_id, datatype_id, namespace_id, name, val_float4_array)
    VALUES
        (AssociatePointCloudCellBeamToFeature.point_cloud_cell_id, 12, 1, 'BeamLineSurfaceDistance', (SELECT ARRAY_AGG(temp_enriched_point_cloud_data.beam_line_surface_distance ORDER BY point_index) FROM temp_enriched_point_cloud_data));

    INSERT INTO sensordb.point_cloud_cell_data
        (point_cloud_cell_id, datatype_id, namespace_id, name, val_float4_array)
    VALUES
        (AssociatePointCloudCellBeamToFeature.point_cloud_cell_id, 12, 1, 'SurfaceZenithAngle', (SELECT ARRAY_AGG(temp_enriched_point_cloud_data.surface_zenith_angle ORDER BY point_index) FROM temp_enriched_point_cloud_data));

    INSERT INTO sensordb.point_cloud_cell_data
        (point_cloud_cell_id, datatype_id, namespace_id, name, val_float4_array)
    VALUES
        (AssociatePointCloudCellBeamToFeature.point_cloud_cell_id, 12, 1, 'SurfaceAzimuthAngle', (SELECT ARRAY_AGG(temp_enriched_point_cloud_data.surface_azimuth_angle ORDER BY point_index) FROM temp_enriched_point_cloud_data));

    INSERT INTO sensordb.point_cloud_cell_data
        (point_cloud_cell_id, datatype_id, namespace_id, name, val_float4_array)
    VALUES
        (AssociatePointCloudCellBeamToFeature.point_cloud_cell_id, 12, 1, 'ReflectionLinePlaneIntersectionParameter', (SELECT ARRAY_AGG(temp_enriched_point_cloud_data.reflection_line_plane_intersection_parameter ORDER BY point_index) FROM temp_enriched_point_cloud_data));


    count := (SELECT COUNT(*)::INT FROM temp_association_candidates);

    DROP INDEX idx_temp_feature_geometry_data_geometry;
    DROP TABLE temp_point_cloud_data;
    DROP TABLE temp_feature_geometry_data;
    DROP TABLE temp_association_candidates;
    DROP TABLE temp_association_confirmed;
    DROP TABLE temp_enriched_point_cloud_data;

    RETURN count;
END;
$$
LANGUAGE plpgsql;
