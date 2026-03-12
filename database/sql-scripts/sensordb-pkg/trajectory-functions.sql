CREATE OR REPLACE FUNCTION sensordb_pkg.interpolate_trajectory_pose(
    p_trajectory_id INT4,
    p_timestamp_sec INT8,
    p_timestamp_nanosec INT4
)
RETURNS TABLE (
    interpolated_position geometry(PointZ),
    interpolated_orientation sensordb.quaternion
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_domain sensordb.trajectory_domain;
BEGIN
    SELECT domain
    INTO v_domain
    FROM sensordb.trajectory
    WHERE id = p_trajectory_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Trajectory % not found', p_trajectory_id;
    END IF;

    -- Dispatch to the domain-specific handler
    IF v_domain = 'timed' THEN
        RETURN QUERY
            SELECT * FROM sensordb_pkg.interpolate_timed_trajectory(p_trajectory_id, p_timestamp_sec, p_timestamp_nanosec);
    ELSIF v_domain = 'sequence' THEN
        RETURN QUERY
            SELECT * FROM sensordb_pkg.interpolate_sequence_trajectory(p_trajectory_id, p_timestamp_sec, p_timestamp_nanosec);
    ELSE
        RAISE EXCEPTION 'Unsupported domain: %', v_domain;
    END IF;
END;
$$;


CREATE OR REPLACE FUNCTION sensordb_pkg.interpolate_timed_trajectory(
    p_trajectory_id INT4,
    p_timestamp_sec INT8,
    p_timestamp_nanosec INT4
)
RETURNS TABLE (
    interpolated_position geometry(PointZ),
    interpolated_orientation sensordb.quaternion
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_interp sensordb.interpolation_type;
    v_extrap sensordb.extrapolation_type;
    v_prev sensordb.trajectory_pose;
    v_next sensordb.trajectory_pose;
    v_query_time NUMERIC := p_timestamp_sec + p_timestamp_nanosec / 1e9;
BEGIN
    SELECT interpolation_type, extrapolation_type
    INTO v_interp, v_extrap
    FROM sensordb.trajectory
    WHERE id = p_trajectory_id;

    -- Load previous and next poses
    v_prev := sensordb_pkg.find_prev_pose(p_trajectory_id, p_timestamp_sec, p_timestamp_nanosec);
    v_next := sensordb_pkg.find_next_pose(p_trajectory_id, p_timestamp_sec, p_timestamp_nanosec);

    -- Exact match
    IF v_prev.id IS NOT NULL AND
       v_prev.timestamp_sec = p_timestamp_sec AND
       v_prev.timestamp_nanosec = p_timestamp_nanosec
    THEN
        interpolated_position := v_prev.position;
        interpolated_orientation := v_prev.orientation;
        RETURN NEXT;
        RETURN;
    END IF;

    -- Interpolation between two poses
    IF v_prev.id IS NOT NULL AND v_next.id IS NOT NULL THEN
        IF v_interp = 'step' THEN
            RETURN QUERY
                SELECT v_prev.position, v_prev.orientation;
        ELSIF v_interp = 'linear' THEN
            RETURN QUERY
                SELECT * FROM sensordb_pkg.interpolate_linear_pose(v_prev, v_next, v_query_time);
        END IF;
        RETURN;
    END IF;

    -- Extrapolation
    IF v_extrap = 'constant' THEN
        RETURN QUERY
            SELECT * FROM sensordb_pkg.extrapolate_constant_pose(v_prev, v_next);
        RETURN;
    END IF;

    -- Fallback: static pose without timestamp
    RETURN QUERY
        SELECT position, orientation
        FROM sensordb.trajectory_pose
        WHERE trajectory_id = p_trajectory_id
          AND timestamp_sec IS NULL
          AND timestamp_nanosec IS NULL
        LIMIT 1;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'No poses found for trajectory %', p_trajectory_id;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION sensordb_pkg.interpolate_sequence_trajectory(
    p_trajectory_id INT4,
    p_timestamp_sec INT8,
    p_timestamp_nanosec INT4  -- ignored for sequence
)
RETURNS TABLE (
    interpolated_position geometry(PointZ),
    interpolated_orientation sensordb.quaternion
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_prev sensordb.trajectory_pose;
    v_next sensordb.trajectory_pose;
BEGIN
    -- For sequence domain, use step interpolation along sequence index
    SELECT *
    INTO v_prev
    FROM sensordb.trajectory_pose
    WHERE trajectory_id = p_trajectory_id
    ORDER BY sequence_index DESC
    LIMIT 1;

    IF v_prev.id IS NOT NULL THEN
        interpolated_position := v_prev.position;
        interpolated_orientation := v_prev.orientation;
        RETURN NEXT;
        RETURN;
    END IF;

    RAISE EXCEPTION 'No sequence poses found for trajectory %', p_trajectory_id;
END;
$$;

CREATE OR REPLACE FUNCTION sensordb_pkg.interpolate_linear_pose(
    p_prev sensordb.trajectory_pose,
    p_next sensordb.trajectory_pose,
    p_query_time NUMERIC
)
RETURNS TABLE (
    interpolated_position geometry(PointZ),
    interpolated_orientation sensordb.quaternion
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_prev_time NUMERIC := p_prev.timestamp_sec + p_prev.timestamp_nanosec / 1e9;
    v_next_time NUMERIC := p_next.timestamp_sec + p_next.timestamp_nanosec / 1e9;
    v_t NUMERIC := (p_query_time - v_prev_time) / (v_next_time - v_prev_time);
BEGIN
    interpolated_position := ST_SetSRID(ST_MakePoint(
        ST_X(p_prev.position) + v_t * (ST_X(p_next.position) - ST_X(p_prev.position)),
        ST_Y(p_prev.position) + v_t * (ST_Y(p_next.position) - ST_Y(p_prev.position)),
        ST_Z(p_prev.position) + v_t * (ST_Z(p_next.position) - ST_Z(p_prev.position))
    ), ST_SRID(p_prev.position));

    IF p_prev.orientation IS NOT NULL AND p_next.orientation IS NOT NULL THEN
        interpolated_orientation := sensordb_pkg.slerp_quaternion(p_prev.orientation, p_next.orientation, v_t);
    ELSE
        interpolated_orientation := p_prev.orientation;
    END IF;

    RETURN NEXT;
END;
$$;

CREATE OR REPLACE FUNCTION sensordb_pkg.extrapolate_constant_pose(
    p_prev sensordb.trajectory_pose,
    p_next sensordb.trajectory_pose
)
RETURNS TABLE (
    extrapolated_position geometry(PointZ),
    extrapolated_orientation sensordb.quaternion
)
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_prev.id IS NULL AND p_next.id IS NOT NULL THEN
        RETURN QUERY
            SELECT p_next.position, p_next.orientation;
    ELSIF p_prev.id IS NOT NULL AND p_next.id IS NULL THEN
        RETURN QUERY
            SELECT p_prev.position, p_prev.orientation;
    ELSIF p_prev.id IS NOT NULL AND p_next.id IS NOT NULL THEN
        RAISE EXCEPTION 'Extrapolation failed: previous and next poses are available';
    ELSE
        RAISE EXCEPTION 'Extrapolation failed: no pose available';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION sensordb_pkg.find_prev_pose(
    p_trajectory_id INT4,
    p_timestamp_sec INT8,
    p_timestamp_nanosec INT4
)
RETURNS sensordb.trajectory_pose
LANGUAGE sql AS $$
    SELECT *
    FROM sensordb.trajectory_pose
    WHERE trajectory_id = $1
      AND (timestamp_sec, timestamp_nanosec) <= ($2, $3)
    ORDER BY timestamp_sec DESC, timestamp_nanosec DESC
    LIMIT 1;
$$;

CREATE OR REPLACE FUNCTION sensordb_pkg.find_next_pose(
    p_trajectory_id INT4,
    p_timestamp_sec INT8,
    p_timestamp_nanosec INT4
)
RETURNS sensordb.trajectory_pose
LANGUAGE sql AS $$
    SELECT *
    FROM sensordb.trajectory_pose
    WHERE trajectory_id = $1
      AND (timestamp_sec, timestamp_nanosec) > ($2, $3)
    ORDER BY timestamp_sec ASC, timestamp_nanosec ASC
    LIMIT 1;
$$;


-- Helper function for quaternion SLERP (Spherical Linear Interpolation)
CREATE OR REPLACE FUNCTION sensordb_pkg.slerp_quaternion(
    q1 sensordb.quaternion,
    q2 sensordb.quaternion,
    t FLOAT8
)
RETURNS sensordb.quaternion
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    result sensordb.quaternion;
    q2_adjusted sensordb.quaternion;
    c_hang FLOAT8;  -- cosine of half angle (dot product)
    s_hang FLOAT8;  -- sine of half angle
    hang FLOAT8;    -- half angle
    ta FLOAT8;      -- weight for q1
    tb FLOAT8;      -- weight for q2
    magnitude FLOAT8;
    epsilon FLOAT8 := 1e-6;
BEGIN
    -- Calculate dot product (cosine of half angle)
    c_hang := q1.x * q2.x + q1.y * q2.y + q1.z * q2.z + q1.w * q2.w;

    -- If dot product is negative, negate q2 to take the shorter path
    -- This matches: if self.coords.dot(&other.coords) < T::zero()
    IF c_hang < 0.0 THEN
        q2_adjusted := ROW(-q2.x, -q2.y, -q2.z, -q2.w)::sensordb.quaternion;
        c_hang := -c_hang;
    ELSE
        q2_adjusted := q2;
    END IF;

    -- If quaternions are identical (or nearly so), return q1
    IF c_hang >= 1.0 THEN
        RETURN q1;
    END IF;

    -- Calculate half angle
    hang := acos(c_hang);

    -- Calculate sine of half angle using identity: sin²(x) + cos²(x) = 1
    s_hang := sqrt(1.0 - c_hang * c_hang);

    -- If sine is too small, quaternions are nearly opposite (180 degrees apart)
    -- Return NULL to indicate interpolation is not well-defined
    -- This matches the epsilon check in the Rust code
    IF s_hang < epsilon THEN
        RETURN NULL;
    END IF;

    -- Standard SLERP formula
    ta := sin((1.0 - t) * hang) / s_hang;
    tb := sin(t * hang) / s_hang;

    result := ROW(
        ta * q1.x + tb * q2_adjusted.x,
        ta * q1.y + tb * q2_adjusted.y,
        ta * q1.z + tb * q2_adjusted.z,
        ta * q1.w + tb * q2_adjusted.w
    )::sensordb.quaternion;

    -- Normalize the result quaternion
    magnitude := sqrt(
        result.x * result.x +
        result.y * result.y +
        result.z * result.z +
        result.w * result.w
    );

    IF magnitude > epsilon THEN
        result := ROW(
            result.x / magnitude,
            result.y / magnitude,
            result.z / magnitude,
            result.w / magnitude
        )::sensordb.quaternion;
    END IF;

    RETURN result;
END;
$$;
