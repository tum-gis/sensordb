CREATE OR REPLACE FUNCTION sensordb_pkg.get_point_cloud_attributes(
    point_cloud_id INTEGER
)
RETURNS TABLE (
    name TEXT,
    datatype_id INTEGER,
    namespace_id INTEGER,
    is_consistent BOOLEAN
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT point_cloud_cell_data.name,
           point_cloud_cell_data.datatype_id,
           point_cloud_cell_data.namespace_id,
           COUNT(DISTINCT point_cloud_cell.id) = (
               SELECT COUNT(id)
               FROM sensordb.point_cloud_cell
               WHERE point_cloud_cell.point_cloud_id = get_point_cloud_attributes.point_cloud_id
           ) AS is_consistent
    FROM sensordb.point_cloud_cell_data
    JOIN sensordb.point_cloud_cell ON point_cloud_cell_data.point_cloud_cell_id = point_cloud_cell.id
    WHERE point_cloud_cell.point_cloud_id = get_point_cloud_attributes.point_cloud_id
    GROUP BY point_cloud_cell_data.name, point_cloud_cell_data.datatype_id, point_cloud_cell_data.namespace_id;
END;
$$;
