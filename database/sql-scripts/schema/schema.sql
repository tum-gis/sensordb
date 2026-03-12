CREATE TYPE sensordb.quaternion AS (
    x FLOAT8,
    y FLOAT8,
    z FLOAT8,
    w FLOAT8
);

CREATE TABLE sensordb.namespace (
    id INT4 PRIMARY KEY,
    alias TEXT
);

CREATE TABLE sensordb.datatype (
    id INT4 PRIMARY KEY,
    typename TEXT
);

CREATE TABLE sensordb.database_srs (
    id SERIAL PRIMARY KEY,
    srid INT4 NOT NULL,
    srs_name TEXT
);

CREATE TABLE sensordb.feature_geometry_data (
    id BIGSERIAL PRIMARY KEY,
    geometry_data_id INT8 NOT NULL,
    feature_id INT8 NOT NULL,
    geometry geometry(PolygonZ) NOT NULL,
    normal_vector geometry(PointZ),
    centroid geometry(PointZ)
);
CREATE INDEX idx_feature_geometry_data_geometry ON sensordb.feature_geometry_data USING gist(geometry gist_geometry_ops_nd);
CREATE INDEX idx_feature_geometry_data_feature_id ON sensordb.feature_geometry_data(feature_id);

CREATE TABLE sensordb.platform (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE CHECK (char_length(name) > 0),
    platform_type TEXT CHECK (platform_type IN ('vehicle', 'uav', 'tripod', 'static')),
    description TEXT CHECK (char_length(description) > 0)
);

CREATE TABLE sensordb.sensor (
    id SERIAL PRIMARY KEY,
    platform_id INT4 NOT NULL REFERENCES sensordb.platform(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    manufacturer TEXT,
    type TEXT NOT NULL CHECK (type IN ('lidar', 'camera', 'radar')),
    model_number TEXT,
    specification JSONB,
    UNIQUE (platform_id, name, type)
);

CREATE TABLE sensordb.campaign (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE CHECK (char_length(name) > 0)
);

CREATE TABLE sensordb.mission (
    id SERIAL PRIMARY KEY,
    campaign_id INT4 NOT NULL REFERENCES sensordb.campaign(id) ON DELETE CASCADE,
    name TEXT NOT NULL CHECK (char_length(name) > 0),
    UNIQUE (campaign_id, name)
);

CREATE TABLE sensordb.recording (
    id SERIAL PRIMARY KEY,
    parent_id INT4 REFERENCES sensordb.recording(id) ON DELETE CASCADE,
    mission_id INT4 NOT NULL REFERENCES sensordb.mission(id) ON DELETE CASCADE,
    platform_id INT4 REFERENCES sensordb.platform(id) ON DELETE CASCADE,
    sensor_id INT4 REFERENCES sensordb.sensor(id) ON DELETE CASCADE,
    name TEXT NOT NULL CHECK (char_length(name) > 0),
    start_date TIMESTAMPTZ,
    end_date TIMESTAMPTZ CHECK (end_date >= start_date),
    UNIQUE (mission_id, platform_id, sensor_id, name)
);

CREATE TYPE sensordb.trajectory_domain AS ENUM ('timed', 'sequence');
CREATE TYPE sensordb.interpolation_type AS ENUM ('step', 'linear');
CREATE TYPE sensordb.extrapolation_type AS ENUM ('constant');

CREATE TABLE sensordb.trajectory (
    id SERIAL PRIMARY KEY,
    recording_id INT4 NOT NULL REFERENCES sensordb.recording(id) ON DELETE CASCADE,
    domain sensordb.trajectory_domain NOT NULL,
    interpolation_type sensordb.interpolation_type NOT NULL,
    extrapolation_type sensordb.extrapolation_type NOT NULL
);

CREATE TABLE sensordb.trajectory_pose (
    id BIGSERIAL PRIMARY KEY,
    trajectory_id INT4 NOT NULL REFERENCES sensordb.trajectory(id) ON DELETE CASCADE,
    timestamp_sec INT8,
    timestamp_nanosec INT4 CHECK (timestamp_nanosec >= 0 AND timestamp_nanosec < 1000000000),
    sequence_index INT4,
    position geometry(PointZ) NOT NULL,
    orientation sensordb.quaternion
    -- UNIQUE (trajectory_id, timestamp_sec, timestamp_nanosec)
);
CREATE INDEX idx_trajectory_pose_trajectory_timestamp ON sensordb.trajectory_pose(trajectory_id, timestamp_sec, timestamp_nanosec);
CREATE INDEX idx_trajectory_pose_timestamp ON sensordb.trajectory_pose(timestamp_sec, timestamp_nanosec);
CREATE INDEX idx_trajectory_pose_position ON sensordb.trajectory_pose USING gist(position gist_geometry_ops_nd);

CREATE TABLE sensordb.point_cloud (
    id SERIAL PRIMARY KEY,
    recording_id INT4 NOT NULL REFERENCES sensordb.recording(id) ON DELETE CASCADE,
    name TEXT CHECK (name IS NULL OR char_length(name) > 0),
    start_date TIMESTAMPTZ,
    end_date TIMESTAMPTZ CHECK (end_date >= start_date),
    UNIQUE (recording_id, name)
);
CREATE INDEX idx_point_cloud_start_date ON sensordb.point_cloud(start_date);
CREATE INDEX idx_point_cloud_end_date ON sensordb.point_cloud(end_date);

CREATE TABLE sensordb.point_cloud_metadata (
    id SERIAL PRIMARY KEY,
    point_cloud_id INT4 REFERENCES sensordb.recording(id) ON DELETE CASCADE,
    name TEXT NOT NULL CHECK (char_length(name) > 0),
    datatype_id INT4 NOT NULL REFERENCES sensordb.datatype(id),
    namespace_id INT4 NOT NULL REFERENCES sensordb.namespace(id),
	val_int8 INT8,
	val_float8 FLOAT8,
	val_string TEXT,
    UNIQUE (point_cloud_id, namespace_id, name)
);

CREATE TABLE sensordb.point_cloud_cell (
    id SERIAL PRIMARY KEY,
    point_cloud_id INT4 NOT NULL REFERENCES sensordb.point_cloud(id) ON DELETE CASCADE,
    level INT4 NOT NULL,
    x INT4 NOT NULL,
    y INT4 NOT NULL,
    z INT4 NOT NULL,
    start_date TIMESTAMPTZ,
    end_date TIMESTAMPTZ CHECK (end_date >= start_date),
    cell_envelope geometry(GeometryZ) NOT NULL,
    point_envelope geometry(GeometryZ) NOT NULL,
    point_count INT4 NOT NULL
);
CREATE INDEX idx_point_cloud_cell_envelope ON sensordb.point_cloud_cell USING gist(cell_envelope gist_geometry_ops_nd);
CREATE INDEX idx_point_cloud_point_envelope ON sensordb.point_cloud_cell USING gist(point_envelope gist_geometry_ops_nd);


CREATE TABLE sensordb.point_cloud_cell_data (
    id SERIAL PRIMARY KEY,
    point_cloud_cell_id INT4 NOT NULL REFERENCES sensordb.point_cloud_cell(id) ON DELETE CASCADE,
    datatype_id INT4 NOT NULL REFERENCES sensordb.datatype(id),
    namespace_id INT4 NOT NULL REFERENCES sensordb.namespace(id),
    name TEXT NOT NULL,
    val_geometry_multi_point geometry(MultiPointZ),
    val_geometry_multi_line_string geometry(MultiLineStringZ),
    val_geometry_polygon geometry(PolygonZ),
	val_int4 INT4,
	val_int8 INT8,
	val_float4 FLOAT4,
	val_float8 FLOAT8,
	val_string TEXT,
    val_quaternion sensordb.quaternion,
    val_int4_array INT4[],
    val_int8_array INT8[],
    val_float4_array FLOAT4[],
    val_float8_array FLOAT8[],
    val_string_array TEXT[],
    val_quaternion_array sensordb.quaternion[],
    val_geometry_data_id_array INT8[],
    UNIQUE (point_cloud_cell_id, namespace_id, name)
);
CREATE INDEX idx_point_cloud_cell_data_val_geometry_multi_point ON sensordb.point_cloud_cell_data USING gist(val_geometry_multi_point gist_geometry_ops_nd);
CREATE INDEX idx_point_cloud_cell_data_val_geometry_multi_line_string ON sensordb.point_cloud_cell_data USING gist(val_geometry_multi_line_string gist_geometry_ops_nd);
CREATE INDEX idx_point_cloud_cell_data_val_geometry_polygon ON sensordb.point_cloud_cell_data USING gist(val_geometry_polygon gist_geometry_ops_nd);
