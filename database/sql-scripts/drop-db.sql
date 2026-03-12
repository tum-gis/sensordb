DROP INDEX IF EXISTS sensordb.idx_feature_geometry_data_geometry;
DROP INDEX IF EXISTS sensordb.idx_feature_geometry_data_feature_id;

DROP TABLE IF EXISTS sensordb.feature_geometry_data;
DROP TABLE IF EXISTS sensordb.feature_material;

DROP TABLE IF EXISTS sensordb.point_cloud_cell;
DROP TABLE IF EXISTS sensordb.recording;

DROP TABLE IF EXISTS sensordb.campaign;
DROP TABLE IF EXISTS sensordb.sensor;
DROP TABLE IF EXISTS sensordb.platform;

DROP TABLE IF EXISTS sensordb.database_srs;

DROP TABLE IF EXISTS sensordb.feature_lidar_signature_entry;
DROP TABLE IF EXISTS sensordb.feature_lidar_signature;
