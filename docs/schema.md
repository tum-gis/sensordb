# Tables

The tables are organized in modules.

## Acquisition System Module

### Sensor

- Table: `sensor`
- Definition: A sensor represents a physical device that captures spatial, positional, or visual data during a mission (e.g., LiDAR scanner, GNSS receiver, IMU, camera).
- Typical scope: A specific hardware unit with unique calibration parameters and technical specifications.
- Purpose: Track sensor characteristics, calibration data, and metadata required for post-processing and data fusion.
- Examples:
  - "Riegl VUX-1LR LiDAR Scanner (Serial #12345)"
  - "NovAtel PwrPak7 GNSS Receiver"
  - "Sony Alpha 7R IV Camera (Body #A001)"
- Attributes: Manufacturer, model, serial number, calibration parameters, accuracy specifications.

### Platform

- Table: `platform`
- Definition: A sensor platform represents the physical vehicle or mounting system that carries one or more sensors during data collection (e.g., aircraft, vehicle, tripod, UAV).
- Typical scope: A complete mobile or static platform with mounting geometry and motion characteristics.
- Purpose: Define the spatial relationships between sensors (lever arms, boresight angles) and platform dynamics.
- Examples:
  - "Cessna 208B Caravan – Registration D-ABCD"
  - "Survey Vehicle – Ford Ranger #V042"
  - "DJI Matrice 300 RTK – UAV Unit 7"
  - "Static Tripod Mount – Station Alpha"
- Contains: Multiple sensors with defined mounting positions and orientations.
- Attributes: Platform type, registration/ID, sensor mount configurations, motion constraints.

## Operations Module

| Level         | Scope                      | Typical Duration | Example Entity                    | Focus                        |
|---------------|----------------------------|------------------|-----------------------------------|------------------------------|
| **Campaign**  | Project-wide               | Weeks–months     | “Bavarian LiDAR Mapping 2024”     | Overall data collection goal |
| **Mission**   | One sortie or scan session | Hours            | “Flight 05 – North Corridor”      | Operational run              |
| **Recording** | Single sensor stream       | Minutes–hours    | “LiDAR_Airborne_2024-05-06_09:45” | Raw sensor data              |

### Campaign

- Table: `campaign`
- Definition: A campaign represents an entire surveying project or data collection effort for a specific purpose, region, or time period.
- Typical scope: Multiple days or weeks of work, potentially involving several vehicles, aircraft, or sensor systems.
- Examples:
  - “2024 Munich Urban LiDAR Campaign” 
  - “Coastal Erosion Mapping Campaign, Q1 2025” 
- Contains: Many missions, possibly recorded by different platforms (e.g., airborne and terrestrial LiDAR).

### Mission

- Table: `mission`
- Definition: A mission represents a single operational sortie or data collection activity conducted under specific conditions — typically corresponding to a flight, drive, or scan session.
- Typical scope: One run of a vehicle or aircraft over a designated area.
- Purpose: Missions are usually planned with precise trajectories, flight altitudes, speeds, and sensor parameters. 
- Examples:
  - “Mission 03 – Southern Sector Flight (Altitude 900 m, Speed 140 knots)”
  - “Vehicle Mission 05 – City Center Drive (Morning Scan)” 
- Contains: One or several recordings (continuous sensor data streams).

- TODO: Lose? Firmen / Organisation?

### Recording

- Table: `recording`
- Definition: A recording represents a continuous data acquisition session from one or more sensors (LiDAR, GNSS, IMU, cameras, etc.) within a mission.
- Typical scope: Starts when the sensors begin collecting data and ends when recording stops.
- Purpose: Raw data for post-processing (e.g., trajectory correction, point cloud generation).
- Examples:
  - “LiDAR_01_2024-07-15_0830.las”
  - “IMU Recording #42 – Mission 03 Segment A”
- May correspond to: A segment of a trajectory, a particular sensor setup, or a file split.

### Sensor Pose

- Table: `sensor_pose`
- Definition: .

## Point Cloud Module

### Point Cloud

- Table: `point_cloud`
- Definition: A point cloud represents a spatial dataset of discrete 3D points generated from one recording or a defined time slice of it, typically sized to allow efficient processing on a single computer.

### Point Cloud Cell

- Table: `point_cloud_cell`
- Definition: A point cloud cell represents a single octant within the point cloud’s octree hierarchy, providing the spatial subdivision used for efficient storage, indexing, and querying.

### Point Cloud Cell Data

- Table: `point_cloud_cell_data`
- Definition:

> The client is designed to support the maximum field size for each column, meaning that complex data structures—such as multi-point geometries or intensity value arrays—are transmitted as single, atomic requests without additional fragmentation or metadata.

### Point Cloud Cell Metadata

- Table: `point_cloud_metadata`

## Image Module

TODO
