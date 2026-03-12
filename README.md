<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/tum-gis/sensordb/main/assets/logo-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/tum-gis/sensordb/main/assets/logo-light.svg">
    <img alt="3DSensorDB - A geospatial database for storing, managing, and analyzing 3D sensor data."
         src="https://raw.githubusercontent.com/tum-gis/sensordb/main/assets/logo-light.svg"
         width="50%">
  </picture>
</div>

## About

[3DSensorDB](https://www.sensordb.org) is a geospatial database for storing, managing, and analyzing 3D sensor data.
When combined with semantic 3D environment models in CityGML, sensor observations can be linked and enriched with semantic,
topological, geometric, and appearance information.

The system is built on [PostgreSQL](https://www.postgresql.org/)/[PostGIS](https://postgis.net/), the [3D City Database](https://github.com/3dcitydb/3dcitydb) supporting [CityGML 1.0–3.0](https://www.ogc.org/standards/citygml/), and [Rust](https://www.rust-lang.org/) for blazingly fast processing.

> This is an early version of the software.
> For questions regarding its use or if you are interested in the sharded variant for large-scale processing, please contact benedikt.schwab@tum.de.

## Getting Started

You need to have [Docker](https://docs.docker.com/get-started/) and [Rust](https://www.rust-lang.org/learn/get-started)
installed.
Build the database using Docker:

```bash
docker build -t sensordb ./database
```

> On an ARM architecture, run ```docker build --platform linux/amd64 -t sensordb ./database```

Start the database by running the built container:

```bash
docker run --name sensordb -p 5432:5432 -d \
    -e "SRID=25832" \
    -e "SRS_NAME=urn:adv:crs:ETRS89_UTM32*DE_DHHN2016_NH" \
    -e "POSTGRES_DB=sensordb" \
    -e "POSTGRES_USER=postgres" \
    -e "POSTGRES_PASSWORD=changeMe" \
    -e "PROJ_NETWORK=ON" \
    -e "POSTGIS_SFCGAL=true" \
  sensordb
```

> Windows users: Use `^` (Command Prompt) or `` ` `` (PowerShell) instead of `\` for line continuation in multi-line commands.

## Usage

### Importing ROS2 Bags

To import the ROS2 bag to the database, run:

```bash
cargo run -r -- \
    --db-host "localhost" \
    --db-port "5432" \
    --db-name "sensordb" \
    --db-username "postgres" \
    --db-password "changeMe" \
    --db-max-connections 10 \
    import-rosbag \
    --rosbag-directory-path /path/to/rosbag \
    --ecoord-file-path /path/to/additional/ecoord \
    --start-time-offset 20s \
    --total-duration 4s
```

### Importing Point Cloud Datasets

To import point cloud files in the epoint, LAS, LAZ, or E57 format, execute the following command:

```bash
cargo run -r -- \
    --db-host "localhost" \
    --db-port "5432" \
    --db-name "sensordb" \
    --db-username "postgres" \
    --db-password "changeMe" \
    --db-max-connections 5 \
    import-point-cloud \
    --point-cloud-directory-path ${HOME}/Desktop/project/point_clouds
```

If you need to shift the point cloud by an offset, use the [epoint-cli](https://docs.rs/epoint/) before importing:

```bash
cargo install epoint-cli@0.0.1-alpha.13 # replace by the latest version
epoint-cli transform \
  --input-directory ${HOME}/Desktop/project/point_clouds_original \
  --output-directory ${HOME}/Desktop/project/point_clouds \
  --translation 0.0 0.0 0.7551
```

### Importing CityGML Datasets

To import CityGML datasets into the 3DCityDB, which is running in parallel, use the Docker container of
the [citydb-tool](https://github.com/3dcitydb/citydb-tool):

```bash
docker run --rm --net=host --name citydb-tool -i -t \
    -e "CITYDB_HOST=localhost" \
    -e "CITYDB_PORT=5432" \
    -e "CITYDB_NAME=sensordb" \
    -e "CITYDB_USERNAME=postgres" \
    -e "CITYDB_PASSWORD=changeMe" \
    -v "${HOME}/Desktop/project/citygml:/data" \
  3dcitydb/citydb-tool import citygml --no-appearances \
  2021-04-22_HD21_529_Intersections_CityHall_Ingolstadt_enhanced__v3.gml
```

### Generating Sensor Views

If a trajectory of the sensor is available, the sensor views, such as the beams of the LiDAR sensor, can be
reconstructed using the following command:

```bash
cargo run -r -- \
    --db-host "localhost" \
    --db-port "5432" \
    --db-name "sensordb" \
    --db-username "postgres" \
    --db-password "changeMe" \
    --db-max-connections 50 \
    generate-sensor-views \
    --reflection-uncertainty-line-length 1.0
```

> The `reflection-uncertainty-line-length` denotes the length (in meters) of the uncertainty line segment for each
> sensor beam reflection.

### Associating Sensor Observations with Object Surfaces

To associate the individual sensor observations with objects from the semantic model, run:

```bash
cargo run -r -- \
    --db-host "localhost" \
    --db-port "5432" \
    --db-name "sensordb" \
    --db-username "postgres" \
    --db-password "changeMe" \
    --db-max-connections 50 \
    associate \
    --reflection-uncertainty-point-buffer 0.5 \
    --reflection-uncertainty-line-buffer 0.1 \
    --max-reflection-uncertainty-line-intersection-parameter 1.0
```

> **Note:** The `reflection-uncertainty-line-buffer` parameter controls how beams are associated with surfaces:
> - When set to `0.0`: Only beams that directly intersect the surface geometry are included.
> - When set to a value > 0: Beams are also included when an object surface falls within the buffer volume around the
    reflection uncertainty line.

The intersection parameter represents the distance along the reflection uncertainty line where an intersection occurs
with the plane of the surface. The value can also be negative if the reflected point lies behind the surface to be
associated.

> **Note:** The `max-reflection-uncertainty-line-intersection-parameter` prevents associations when beams are
> nearly parallel to surfaces within the buffer volume.
> - When a beam is nearly parallel to a surface, the intersection parameter can become extremely large, as the
    intersection point is projected far beyond the intended beam range.
> - Setting this parameter (e.g., to `1.0`) caps the maximum acceptable intersection distance, filtering out unrealistic
    associations caused by grazing-angle beams or inaccuracies.

### Exporting Point Clouds

To export the associated sensor data, run:

```bash
cargo run -r -- \
    --db-host "localhost" \
    --db-port "5432" \
    --db-name "sensordb" \
    --db-username "postgres" \
    --db-password "changeMe" \
    --db-max-connections 5 \
    export \
    --point-cloud-format xyz \
    --directory-path ${HOME}/Desktop/project/export
```

### Acknowledgements

Sincere thanks to the [development partners](https://docs.3dcitydb.org/latest/contributors/) of the [3D City Database](https://www.3dcitydb.org/), 
which serves as both a reference for this project and enables linking 3D sensor data to semantic models in CityGML.
