CREATE OR REPLACE FUNCTION sensordb_pkg.ST_3DNormalVector(
    polygon geometry(PolygonZ)
)  RETURNS geometry(PointZ) AS
$$
DECLARE
    enclosed_exterior_ring geometry;
    num_points int;
    current_point geometry(PointZ);
    next_point geometry(PointZ);
    normal_x double precision = 0;
    normal_y double precision = 0;
    normal_z double precision = 0;
    length double precision;
BEGIN
    IF NOT ST_Dimension(polygon) = 2 THEN
        -- RAISE EXCEPTION 'Inputs must be 2D geometries';
        RETURN NULL;
    END IF;

    enclosed_exterior_ring := ST_Force3D(ST_ExteriorRing(polygon));
    IF NOT ST_IsClosed(enclosed_exterior_ring) THEN
        enclosed_exterior_ring := ST_AddPoint(enclosed_exterior_ring, ST_PointN(enclosed_exterior_ring, 1));
    END IF;
    num_points := ST_NPoints(enclosed_exterior_ring);
    --RAISE NOTICE 'number of original points %', num_points;
    IF num_points IS NULL OR num_points < 3 THEN
        --RAISE EXCEPTION 'Inputs geometry must have at least 3 points';
        RETURN NULL;
    END IF;

    FOR i IN 1..(num_points-1) LOOP
            -- Get the i-th point of the exterior ring
            current_point := ST_PointN(enclosed_exterior_ring, i);
            next_point := ST_PointN(enclosed_exterior_ring, i+1);

            --RAISE NOTICE 'current_point %: (X: %, Y: %, Z: %), next_point: (X: %, Y: %, Z: %)',
            --    i,
            --    ST_X(current_point), ST_Y(current_point), ST_Z(current_point),
            --    ST_X(next_point), ST_Y(next_point), ST_Z(next_point);
            normal_x := normal_x + (ST_Y(current_point) - ST_Y(next_point)) * (ST_Z(current_point) + ST_Z(next_point));
            normal_y := normal_y + (ST_Z(current_point) - ST_Z(next_point)) * (ST_X(current_point) + ST_X(next_point));
            normal_z := normal_z + (ST_X(current_point) - ST_X(next_point)) * (ST_Y(current_point) + ST_Y(next_point));
    END LOOP;
    length := sqrt(normal_x^2 + normal_y^2 + normal_z^2);
    -- RAISE NOTICE 'length: %', length;
    IF length IS NULL OR length = 0 THEN
        RETURN NULL;
    END IF;

    RETURN ST_MakePoint(normal_x/length, normal_y/length, normal_z/length);
END;
$$
LANGUAGE plpgsql;



-- https://stackoverflow.com/a/24708185
CREATE OR REPLACE FUNCTION sensordb_pkg.ST_CrossProduct(point_a geometry, point_b geometry)
  RETURNS geometry AS
$BODY$SELECT ST_SetSRID(ST_MakePoint(
  a2 * b3 - a3 * b2,
  a3 * b1 - a1 * b3,
  a1 * b2 - a2 * b1), ST_SRID($1))
FROM (SELECT
  ST_X($1) AS a1, ST_Y($1) AS a2, COALESCE(ST_Z($1), 0.0) AS a3,
  ST_X($2) AS b1, ST_Y($2) AS b2, COALESCE(ST_Z($2), 0.0) AS b3
) AS f$BODY$
LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION sensordb_pkg.ST_DotProduct(point_a geometry, point_b geometry)
  RETURNS double precision AS
$BODY$SELECT a1 * b1 + a2 * b2 + a3 * b3
FROM (SELECT
  ST_X($1) AS a1, ST_Y($1) AS a2, COALESCE(ST_Z($1), 0.0) AS a3,
  ST_X($2) AS b1, ST_Y($2) AS b2, COALESCE(ST_Z($2), 0.0) AS b3
) AS f$BODY$
LANGUAGE sql IMMUTABLE;



-- Returns angle between vectors in rad between [0, PI].
CREATE OR REPLACE FUNCTION sensordb_pkg.ST_3DAngleBetweenVectors(
    vector_a geometry(PointZ),
    vector_b geometry(PointZ)
)  RETURNS double precision AS
$$
DECLARE
    cross_product geometry(PointZ);
    dot_product double precision;
    cross_product_norm double precision;
BEGIN
    IF vector_a IS NULL OR vector_b IS NULL THEN
        RETURN NULL;
    END IF;

    -- https://de.mathworks.com/matlabcentral/answers/2092961-how-to-calculate-the-angle-between-two-3d-vectors
    -- atan2(norm(cross(u,v)), dot(u,v))
    cross_product := sensordb_pkg.ST_CrossProduct(vector_a, vector_b);
    --RAISE NOTICE 'cross_product: (X: %, Y: %, Z: %)', ST_X(cross_product), ST_Y(cross_product), ST_Z(cross_product);
    cross_product_norm := sqrt(ST_X(cross_product)^2 + ST_Y(cross_product)^2 + ST_Z(cross_product)^2);
    --RAISE NOTICE 'cross_product_norm: %', cross_product_norm;
    dot_product := sensordb_pkg.ST_DotProduct(vector_a, vector_b);

    RETURN atan2(cross_product_norm, dot_product);
END;
$$
LANGUAGE plpgsql;


-- Calculates the angle between the normal vector of a polygon and a given line.
-- Returns an angle in the range [0, PI), where:
--  - 0 indicates that the line is perpendicular to the polygon.
--  - PI/2 indicates that the line is parallel to the polygon.
--  - PI indicates that the line is perpendicular to the polygon but oriented in the opposite direction (from the backside).
CREATE OR REPLACE FUNCTION sensordb_pkg.ST_3DAngleBetweenPolygonNormalAndLine(
    polygon geometry(PolygonZ),
    line geometry(LinestringZ)
)  RETURNS double precision AS
$$
DECLARE
    polygon_normal_vector geometry(PointZ);
    line_start_point geometry(PointZ);
    line_end_point geometry(PointZ);
    line_vector geometry(PointZ);
BEGIN
    IF polygon IS NULL OR line IS NULL THEN
        RETURN NULL;
    END IF;

    polygon_normal_vector := sensordb_pkg.ST_3DNormalVector(polygon);
    line_start_point := ST_StartPoint(line);
    line_end_point := ST_EndPoint(line);
    line_vector := ST_MakePoint(ST_X(line_end_point) - ST_X(line_start_point),
                                ST_Y(line_end_point) - ST_Y(line_start_point),
                                ST_Z(line_end_point) - ST_Z(line_start_point));

    RETURN PI() - sensordb_pkg.ST_3DAngleBetweenVectors(polygon_normal_vector, line_vector);
END;
$$
LANGUAGE plpgsql;


-- Calculates the zenith/incident angle between a polygon and an incoming line.
-- Returns an angle in the range [0,PI/2] or null, where:
--  - 0 indicates that the line is perpendicular to the polygon.
--  - PI/2 indicates that the line is parallel to the polygon.
--  - null indicates that the line is approaching from the backside of the polygon.
CREATE OR REPLACE FUNCTION sensordb_pkg.ST_3DZenithAngle(
    polygon geometry(PolygonZ),
    line geometry(LinestringZ)
)  RETURNS double precision AS
$$
DECLARE
    angle_between_polygon_and_line double precision;
BEGIN
    angle_between_polygon_and_line := sensordb_pkg.ST_3DAngleBetweenPolygonNormalAndLine(polygon, line);
    IF angle_between_polygon_and_line IS NULL OR angle_between_polygon_and_line > (PI() / 2.0) THEN
        RETURN NULL;
    END IF;

    RETURN angle_between_polygon_and_line;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION sensordb_pkg.ST_3DPolygonCentroid(
    polygon geometry(PolygonZ)
)  RETURNS geometry(PointZ) AS
$$
DECLARE
    exterior_ring geometry;
    num_points int;
    current_point geometry(PointZ);
    sum_x double precision := 0;
    sum_y double precision := 0;
    sum_z double precision := 0;
BEGIN
    IF polygon IS NULL THEN
        RETURN NULL;
    END IF;

    exterior_ring := ST_Force3D(ST_ExteriorRing(polygon));
    IF ST_IsClosed(exterior_ring) THEN
        exterior_ring := ST_RemovePoint(exterior_ring, ST_NPoints(exterior_ring) - 1);
    END IF;

    num_points := ST_NPoints(exterior_ring);

    FOR i IN 1..num_points LOOP
            -- Get the i-th point of the exterior ring
            current_point := ST_PointN(exterior_ring, i);

            --RAISE NOTICE 'current_point %: (X: %, Y: %, Z: %)',
            --    i, ST_X(current_point), ST_Y(current_point), ST_Z(current_point);
            sum_x := sum_x + ST_X(current_point);
            sum_y := sum_y + ST_Y(current_point);
            sum_z := sum_z + ST_Z(current_point);
    END LOOP;

    RETURN ST_MakePoint(sum_x/num_points, sum_y/num_points, sum_z/num_points);
END;
$$
LANGUAGE plpgsql;


-- Returns an orthogonal basis from a given normal vector using the Gram-Schmidt process.
-- https://en.wikipedia.org/wiki/Gram%E2%80%93Schmidt_process
CREATE OR REPLACE FUNCTION sensordb_pkg.ST_3DGetBasisFromNormal(
    normal geometry(PointZ)
)  RETURNS TABLE(u geometry(PointZ), v geometry(PointZ), n geometry(PointZ)) AS
$$
DECLARE
    nx double precision := ST_X(normal);
    ny double precision := ST_Y(normal);
    nz double precision := ST_Z(normal);
    magnitude double precision;
    a geometry(PointZ);
    u geometry(PointZ);
    v geometry(PointZ);
    epsilon double precision := 0.01;

    -- Helper variables
    dot_product double precision;
BEGIN
    IF normal IS NULL THEN
        RETURN QUERY SELECT NULL, NULL, NULL;
    END IF;

    -- Normalize the normal vector
    magnitude := SQRT(nx * nx + ny * ny + nz * nz);
    IF magnitude = 0 THEN
        RAISE EXCEPTION 'Cannot create basis from a zero vector';
    END IF;
    normal := ST_MakePoint(nx / magnitude, ny / magnitude, nz / magnitude);
    -- RAISE NOTICE 'normalized normal: (X: %, Y: %, Z: %)', ST_X(normal), ST_Y(normal), ST_Z(normal);

    -- Choose an arbitrary vector based on proximity to the z-axis
    IF SQRT(ST_X(normal) * ST_X(normal) + ST_Y(normal) * ST_Y(normal)) < epsilon THEN
        -- If close to z-axis, use x-axis as an arbitrary vector
        a := ST_MakePoint(1, 0, 0);
    ELSE
        -- Otherwise, use z-axis as an arbitrary vector
        a := ST_MakePoint(0, 0, 1);
    END IF;
    -- RAISE NOTICE 'a: (X: %, Y: %, Z: %)', ST_X(a), ST_Y(a), ST_Z(a);

    -- Calculate the first orthogonal vector u
    dot_product := ST_X(a) * ST_X(normal) + ST_Y(a) * ST_Y(normal) + ST_Z(a) * ST_Z(normal);
    u := ST_MakePoint(
        ST_X(a) - dot_product * ST_X(normal),
        ST_Y(a) - dot_product * ST_Y(normal),
        ST_Z(a) - dot_product * ST_Z(normal)
    );
    -- RAISE NOTICE 'u: (X: %, Y: %, Z: %)', ST_X(u), ST_Y(u), ST_Z(u);

    -- Normalize u
    magnitude := SQRT(ST_X(u) * ST_X(u) + ST_Y(u) * ST_Y(u) + ST_Z(u) * ST_Z(u));
    u := ST_MakePoint(ST_X(u) / magnitude, ST_Y(u) / magnitude, ST_Z(u) / magnitude);
    -- RAISE NOTICE 'normalized u: (X: %, Y: %, Z: %)', ST_X(u), ST_Y(u), ST_Z(u);

    -- Calculate the second orthogonal vector v as the cross-product of normalized_normal and u
    v := sensordb_pkg.ST_CrossProduct(normal, u);
    -- RAISE NOTICE 'v: (X: %, Y: %, Z: %)', ST_X(v), ST_Y(v), ST_Z(v);

    -- Normalize v
    magnitude := SQRT(ST_X(v) * ST_X(v) + ST_Y(v) * ST_Y(v) + ST_Z(v) * ST_Z(v));
    v := ST_MakePoint(ST_X(v) / magnitude, ST_Y(v) / magnitude, ST_Z(v) / magnitude);

    -- Return the orthogonal basis (u, v, n)
    RETURN QUERY SELECT u, v, normal;
END;
$$
LANGUAGE plpgsql;


-- Calculates the azimuth angle between a 3D polygon and an incoming line in 3D space.
-- Returns an angle in the range [0, 2*PI) or null for a degenerate polygon or line.
CREATE OR REPLACE FUNCTION sensordb_pkg.ST_3DAzimuthAngle(
    polygon geometry(PolygonZ),
    line geometry(LinestringZ)
)  RETURNS double precision AS
$$
DECLARE
    line_start geometry(PointZ) := ST_StartPoint(line);
    line_end geometry(PointZ) := ST_EndPoint(line);
    polygon_centroid geometry(PointZ) := sensordb_pkg.ST_3DPolygonCentroid(polygon);
    polygon_normal geometry(PointZ) := sensordb_pkg.ST_3DNormalVector(polygon);
    plane_projected_start geometry(PointZ);

    basis_u geometry(PointZ);
    basis_v geometry(PointZ);
    basis_n geometry(PointZ);

    -- Variables to hold transformed coordinates
    distance double precision;

    line_end_translated geometry(PointZ);
    line_end_in_uvn_basis geometry(PointZ);
BEGIN
    IF polygon IS NULL OR line IS NULL THEN
        RETURN NULL;
    END IF;

    -- Calculate distance and project line start onto the polygon plane
    distance := (ST_X(line_start) - ST_X(polygon_centroid)) * ST_X(polygon_normal) +
                (ST_Y(line_start) - ST_Y(polygon_centroid)) * ST_Y(polygon_normal) +
                (ST_Z(line_start) - ST_Z(polygon_centroid)) * ST_Z(polygon_normal);
    plane_projected_start := ST_MakePoint(
        ST_X(line_start) - distance * ST_X(polygon_normal),
        ST_Y(line_start) - distance * ST_Y(polygon_normal),
        ST_Z(line_start) - distance * ST_Z(polygon_normal));
    --RAISE NOTICE 'Plane projected line start point: %, %, %',
    --    ST_X(plane_projected_start), ST_Y(plane_projected_start), ST_Z(plane_projected_start);

    -- Get the orthogonal basis vectors for the polygon plane
    SELECT u, v, n
    INTO basis_u, basis_v, basis_n
    FROM sensordb_pkg.ST_3DGetBasisFromNormal(polygon_normal);
    -- RAISE NOTICE 'New basis: u=%, v=%, n=%', st_astext(basis_u), st_astext(basis_v), st_astext(basis_n);


    -- Translate line end relative to the plane-projected start
    line_end_translated := ST_MakePoint(
        ST_X(line_end) - ST_X(plane_projected_start),
        ST_Y(line_end) - ST_Y(plane_projected_start),
        ST_Z(line_end) - ST_Z(plane_projected_start)
    );
    -- RAISE NOTICE 'Transformed moved point: (x: %, y: %, z: %)', ST_X(line_end_translated), ST_Y(line_end_translated), ST_Z(line_end_translated);

    -- Project translated line end into the (u, v, n) basis
    line_end_in_uvn_basis := ST_MakePoint(
            (ST_X(basis_u) * ST_X(line_end_translated)) + (ST_Y(basis_u) * ST_Y(line_end_translated)) + (ST_Z(basis_u) * ST_Z(line_end_translated)),
            (ST_X(basis_v) * ST_X(line_end_translated)) + (ST_Y(basis_v) * ST_Y(line_end_translated)) + (ST_Z(basis_v) * ST_Z(line_end_translated)),
            (ST_X(basis_n) * ST_X(line_end_translated)) + (ST_Y(basis_n) * ST_Y(line_end_translated)) + (ST_Z(basis_n) * ST_Z(line_end_translated))
    );
    -- RAISE NOTICE 'Transformed start point in new basis: (u: %, v: %, n: %)', ST_X(line_end_in_uvn_basis), ST_Y(line_end_in_uvn_basis), ST_Z(line_end_in_uvn_basis);

    -- Calculate the azimuth angle in range [0, 2π) using atan2 for stability
    RETURN MOD((2 * PI() + atan2(ST_Y(line_end_in_uvn_basis), ST_X(line_end_in_uvn_basis)))::numeric, (2 * PI())::numeric);
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sensordb_pkg.ST_3DCartesianToSpherical(
    point geometry(PointZ)
)  RETURNS TABLE(spherical_azimuth double precision, spherical_elevation double precision, spherical_range double precision) AS
$$
DECLARE
    point_x double precision := ST_X(point);
    point_y double precision := ST_Y(point);
    point_z double precision := ST_Z(point);

    spherical_azimuth double precision;
    spherical_elevation double precision;
    spherical_range double precision;
BEGIN
    IF point IS NULL THEN
        RETURN QUERY SELECT NULL::double precision, NULL::double precision, NULL::double precision;
    END IF;
    IF point_x = 0 AND point_y = 0 AND point_z = 0 THEN
        RETURN QUERY SELECT
            NULL::double precision as spherical_azimuth,
            NULL::double precision as spherical_elevation,
            0.0::double precision as spherical_range;
    END IF;

    spherical_azimuth := ATAN2(point_y, point_x);
    spherical_elevation := ATAN2(point_z, SQRT(point_x^2 + point_y^2));
    spherical_range := SQRT(point_x^2 + point_y^2 + point_z^2);


    RETURN QUERY SELECT spherical_azimuth, spherical_elevation, spherical_range;
END;
$$
LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION sensordb_pkg.ST_ApplyUnitQuaternionRotation(
    point geometry(PointZ),
    quaternion_x double precision,
    quaternion_y double precision,
    quaternion_z double precision,
    quaternion_w double precision
)  RETURNS geometry(PointZ) AS
$$
DECLARE
    point_x double precision := ST_X(point);
    point_y double precision := ST_Y(point);
    point_z double precision := ST_Z(point);

    norm double precision;
    norm_quaternion_x double precision;
    norm_quaternion_y double precision;
    norm_quaternion_z double precision;
    norm_quaternion_w double precision;

    rotated_point_x double precision;
    rotated_point_y double precision;
    rotated_point_z double precision;

BEGIN
    IF point IS NULL THEN
        RETURN NULL;
    END IF;

    -- Normalize the quaternion
    norm := SQRT(quaternion_x^2 + quaternion_y^2 + quaternion_z^2 + quaternion_w^2);
    IF norm = 0 THEN
        RAISE EXCEPTION 'Invalid quaternion: norm is zero';
    END IF;
    norm_quaternion_x := quaternion_x / norm;
    norm_quaternion_y := quaternion_y / norm;
    norm_quaternion_z := quaternion_z / norm;
    norm_quaternion_w := quaternion_w / norm;

    -- https://en.wikipedia.org/wiki/Quaternions_and_spatial_rotation#Quaternion-derived_rotation_matrix
    -- Apply quaternion rotation using the Hamilton product:
    rotated_point_x := (1-2*(norm_quaternion_y^2+norm_quaternion_z^2)) * point_x +
                       2*(norm_quaternion_x*norm_quaternion_y - norm_quaternion_z*norm_quaternion_w) * point_y +
                       2*(norm_quaternion_x*norm_quaternion_z + norm_quaternion_y*norm_quaternion_w) * point_z;

    rotated_point_y := 2*(norm_quaternion_x*norm_quaternion_y + norm_quaternion_z*norm_quaternion_w) * point_x +
                       (1-2*(norm_quaternion_x^2+norm_quaternion_z^2)) * point_y +
                       2*(norm_quaternion_y*norm_quaternion_z - norm_quaternion_x*norm_quaternion_w) * point_z;

    rotated_point_z := 2*(norm_quaternion_x*norm_quaternion_z - norm_quaternion_y*norm_quaternion_w) * point_x +
                       2*(norm_quaternion_y*norm_quaternion_z + norm_quaternion_x*norm_quaternion_w) * point_y +
                       (1-2*(norm_quaternion_x^2+norm_quaternion_y^2)) * point_z;

    RETURN ST_MakePoint(rotated_point_x, rotated_point_y, rotated_point_z);
END;
$$
LANGUAGE plpgsql;


-- https://en.wikipedia.org/wiki/Line%E2%80%93plane_intersection#Algebraic_form
CREATE OR REPLACE FUNCTION sensordb_pkg.ST_3DLinePlaneIntersectionParameter(
    line_point geometry(PointZ),
    line_direction_vector geometry(PointZ),
    plane_point geometry(PolygonZ),
    plane_normal_vector geometry(PolygonZ)
)  RETURNS double precision AS
$$
DECLARE
    magnitude double precision;
    normalized_line_direction_vector geometry(PointZ);
    dot_product_nd double precision;
    numerator double precision;
BEGIN
    IF line_point IS NULL OR line_direction_vector IS NULL OR plane_point IS NULL OR plane_normal_vector IS NULL THEN
        RETURN NULL;
    END IF;

    magnitude := SQRT(ST_X(line_direction_vector) * ST_X(line_direction_vector) + ST_Y(line_direction_vector) * ST_Y(line_direction_vector) + ST_Z(line_direction_vector) * ST_Z(line_direction_vector));
    normalized_line_direction_vector := ST_MakePoint(ST_X(line_direction_vector) / magnitude, ST_Y(line_direction_vector) / magnitude, ST_Z(line_direction_vector) / magnitude);

    dot_product_nd =
        ST_X(plane_normal_vector) * ST_X(normalized_line_direction_vector) +
        ST_Y(plane_normal_vector) * ST_Y(normalized_line_direction_vector) +
        ST_Z(plane_normal_vector) * ST_Z(normalized_line_direction_vector);

    -- If the line is parallel to the plane, return NULL.
    -- In this case the line can either be contained in the plane or have no intersections at all.
    IF dot_product_nd = 0 THEN
        RETURN NULL;
    END IF;

    -- Compute the numerator (dot product of the normal vector with the vector from plane_point to line_point)
    numerator =
        ST_X(plane_normal_vector) * (ST_X(plane_point) - ST_X(line_point)) +
        ST_Y(plane_normal_vector) * (ST_Y(plane_point) - ST_Y(line_point)) +
        ST_Z(plane_normal_vector) * (ST_Z(plane_point) - ST_Z(line_point));

    RETURN numerator / dot_product_nd;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION sensordb_pkg.ST_3DMakeEnvelope(
    lower_corner geometry(PointZ),
    upper_corner geometry(PointZ),
    srid INTEGER
)  RETURNS GEOMETRY AS
$$
DECLARE
    envelope GEOMETRY;
BEGIN
    IF lower_corner IS NULL OR upper_corner IS NULL THEN
        RETURN NULL;
    END IF;

  envelope = ST_SetSRID(ST_MakePolygon(ST_MakeLine(
    ARRAY[
      ST_MakePoint(ST_X(lower_corner), ST_Y(lower_corner), ST_Z(lower_corner)),
      ST_MakePoint(ST_X(upper_corner), ST_Y(lower_corner), ST_Z(lower_corner)),
      ST_MakePoint(ST_X(upper_corner), ST_Y(upper_corner), ST_Z(upper_corner)),
      ST_MakePoint(ST_X(lower_corner), ST_Y(upper_corner), ST_Z(upper_corner)),
      ST_MakePoint(ST_X(lower_corner), ST_Y(lower_corner), ST_Z(lower_corner))
    ]
  )), srid);

    RETURN envelope;
END;
$$
LANGUAGE plpgsql;
