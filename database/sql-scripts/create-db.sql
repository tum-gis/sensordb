\pset footer off
SET client_min_messages TO WARNING;
\set ON_ERROR_STOP ON

\set SRID :srid
\set SRS_NAME :srs_name

-- check if the PostGIS extension is available
SELECT postgis_lib_version() AS postgis_version
\gset

-- check if the provided SRID is supported
\echo
\echo 'Checking spatial reference system for SRID ':SRID' ...'
SET tmp.srid to :"srid";
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM spatial_ref_sys WHERE srid = current_setting('tmp.srid')::int) THEN
    RAISE EXCEPTION 'The SRID % is not supported. To add it manually, see CRS definitions at https://spatialreference.org/.', current_setting('tmp.srid');
  END IF;
END
$$;

-- create schema
CREATE SCHEMA sensordb;

-- create tables, sequences, constraints, indexes
\echo
\echo 'Setting up database schema of 3DSensorDB instance ...'
\ir schema/schema.sql
\ir schema/views.sql

-- populate metadata tables
\ir schema/namespace-instances.sql
\ir schema/datatype-instances.sql


-- create sensordb_pkg schema
\echo
\echo 'Creating additional schema "sensordb_pkg" ...'
CREATE SCHEMA sensordb_pkg;

\ir sensordb-pkg/srs.sql
\ir sensordb-pkg/geometry-functions.sql
\ir sensordb-pkg/association-functions.sql
\ir sensordb-pkg/point-cloud-functions.sql
\ir sensordb-pkg/trajectory-functions.sql


\echo 'Setting spatial reference system of 3DSensorDB instance ...'
SELECT sensordb_pkg.change_schema_srid(:SRID,:'SRS_NAME');

\echo '3DSensorDB instance successfully created.'
