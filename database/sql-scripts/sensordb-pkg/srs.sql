/*******************************************************************
* change_schema_srid
*
* @param target_srid The SRID of the coordinate system to be further used in the database
* @param target_srs_name The SRS_NAME of the coordinate system to be further used in the database
* @param transform Set to 1 if existing data shall be transformed, 0 if not
*******************************************************************/
CREATE OR REPLACE FUNCTION sensordb_pkg.change_schema_srid(
  target_srid INTEGER,
  target_srs_name TEXT,
  transform INTEGER DEFAULT 0) RETURNS SETOF VOID AS
$body$
BEGIN
  -- update entry in database_srs table
  DELETE FROM sensordb.database_srs;
  INSERT INTO sensordb.database_srs (srid, srs_name) VALUES ($1, $2);

  -- change SRID of spatial columns
  --PERFORM sensordb_pkg.change_column_srid(f_table_name, f_geometry_column, coord_dimension, $1, $3, type)
  --FROM geometry_columns
  --WHERE f_table_schema = sensordb_pkg.get_current_schema()
  --  AND f_geometry_column <> 'implicit_geometry';
END;
$body$
LANGUAGE plpgsql;
