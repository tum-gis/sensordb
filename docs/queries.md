# Further Queries

## Trajectories

To retrieve all trajectories with their sensor and platform information:

```sql
SELECT
    trajectory.id,
    sensor.name AS sensor_name,
    platform.name AS platform_name,
    st_makeline(trajectory_pose.position ORDER BY timestamp_sec ASC, timestamp_nanosec ASC) as trajectory_line
FROM sensordb.trajectory
JOIN sensordb.trajectory_pose ON trajectory.id = trajectory_pose.trajectory_id
JOIN sensordb.recording ON trajectory.recording_id = recording.id
LEFT JOIN sensordb.sensor ON recording.sensor_id = sensor.id
LEFT JOIN sensordb.platform ON recording.platform_id = platform.id
GROUP BY trajectory.id, sensor.name, platform.id;
```
