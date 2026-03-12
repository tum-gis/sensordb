CREATE VIEW sensordb.point_cloud_cell_view AS
    WITH cell_bounds AS (
        SELECT
            point_cloud_cell.id AS id,
            point_cloud_id,
            level,
            x,
            y,
            z,
            point_cloud_cell.start_date AS start_date,
            point_cloud_cell.end_date AS end_date,
            cell_envelope AS cell_envelope,
            point_envelope AS point_envelope,
            point_count AS point_count,
            recording.name AS recording_name,
            ST_XMin(point_cloud_cell.cell_envelope) AS x_min,
            ST_YMin(point_cloud_cell.cell_envelope) AS y_min,
            ST_ZMin(point_cloud_cell.cell_envelope) AS z_min,
            ST_XMax(point_cloud_cell.cell_envelope) AS x_max,
            ST_YMax(point_cloud_cell.cell_envelope) AS y_max,
            ST_ZMax(point_cloud_cell.cell_envelope) AS z_max,
            ST_SRID(point_cloud_cell.cell_envelope) AS srid
        FROM sensordb.point_cloud_cell
        JOIN sensordb.point_cloud ON point_cloud_cell.point_cloud_id = point_cloud.id
        JOIN sensordb.recording ON point_cloud.recording_id = recording.id
    )
    SELECT
        id,
        'Name: ' || COALESCE(recording_name::text, 'Unknown') || ', Octant: level=' || level::text || ',x=' || x::text || ',y=' || y::text || ',z=' || z::text AS cell_name,
        recording_name AS recording_name,
        point_cloud_id AS point_cloud_id,
        level AS octant_index_level,
        x AS octant_index_x,
        y AS octant_index_y,
        z AS octant_index_z,
        start_date,
        end_date,
        EXTRACT(EPOCH FROM start_date)::INTEGER AS start_date_timestamp,
        EXTRACT(EPOCH FROM end_date)::INTEGER AS end_date_timestamp,
        (EXTRACT(EPOCH FROM end_date) - EXTRACT(EPOCH FROM start_date))::INTEGER AS duration,
        (x_max - x_min) * (y_max - y_min) * (z_max - z_min) AS volume,
        point_count AS point_count,
        point_count / NULLIF((x_max - x_min) * (y_max - y_min) * (z_max - z_min), 0) AS points_per_cubic_meter,
        x_min,
        y_min,
        z_min,
        x_max,
        y_max,
        z_max,
        point_envelope,
        cell_envelope,
        ST_SetSRID(
            CG_MakeSolid(
                ST_3DMakeBox(
                    ST_MakePoint(x_min, y_min, z_min),
                    ST_MakePoint(x_max, y_max, z_max)
                )
            ),
            srid
        ) AS cell_envelope_box
    FROM cell_bounds;
