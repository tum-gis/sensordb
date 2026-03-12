use crate::Error;
use crate::database::tables::TrajectoryPoseEntry;
use sqlx::{Pool, Postgres, QueryBuilder};

/// Insert a vector of sensor poses into the database
pub async fn insert_trajectory_poses(
    pool: &Pool<Postgres>,
    trajectory_poses: Vec<TrajectoryPoseEntry>,
) -> Result<(), Error> {
    if trajectory_poses.is_empty() {
        return Ok(());
    }

    for current_trajectory_poses_chunk in trajectory_poses.chunks(10_000) {
        insert_trajectory_poses_chunk(pool, current_trajectory_poses_chunk).await?;
    }

    Ok(())
}

async fn insert_trajectory_poses_chunk(
    pool: &Pool<Postgres>,
    trajectory_poses: &[TrajectoryPoseEntry],
) -> Result<(), Error> {
    let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
        "INSERT INTO sensordb.trajectory_pose (trajectory_id, timestamp_sec, timestamp_nanosec, sequence_index, position, orientation) ",
    );

    query_builder.push_values(trajectory_poses, |mut b, pose| {
        b.push_bind(pose.trajectory_id)
            .push_bind(pose.timestamp_sec)
            .push_bind(pose.timestamp_nanosec)
            .push_bind(pose.sequence_index);

        // Handle position (PostGIS PointZ)
        if let Some((x, y, z)) = pose.position {
            b.push(format!(
                "ST_SetSRID(ST_MakePoint({}, {}, {}), (SELECT srid FROM database_srs LIMIT 1))",
                x, y, z
            ));
        } else {
            b.push("NULL");
        }

        // Handle orientation (custom quaternion type)
        if let Some(q) = &pose.orientation {
            b.push(format!(
                "ROW({}, {}, {}, {})::sensordb.quaternion",
                q.x, q.y, q.z, q.w
            ));
        } else {
            b.push("NULL");
        }
    });

    query_builder.build().execute(pool).await?;

    Ok(())
}
