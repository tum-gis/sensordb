use crate::database::datatype::{MissionId, PlatformId, RecordingId};
use crate::database::queries::import_point_cloud::insert_point_cloud_cell;
use crate::database::queries::import_trajectories::insert_trajectory_poses;
use crate::database::tables::{
    ExtrapolationType, InterpolationType, SensorType, TrajectoryDomain, TrajectoryPoseEntry,
};
use crate::database::util::get_progress_bar;
use crate::point_cloud_extensions::ExtendedPointDataColumnType;
use crate::{DatabaseManager, Error};
use chrono::{DateTime, Utc};
use ecoord::octree::StorageMode;
use ecoord::{FrameId, TransformId, TransformTree};
use epoint::PointCloud;
use epoint::octree::PointCloudOctree;
use erosbag::dto::McapMessagePage;
use erosbag::ros_messages::RosMessageType;
use erosbag::{ChannelTopic, ChunkId, FileName};
use indicatif::{MultiProgress, ProgressBar};
use itertools::Itertools;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tracing::log::info;
use tracing::warn;
use walkdir::WalkDir;

impl DatabaseManager {
    pub async fn import_rosbag(
        &self,
        rosbag: erosbag::Rosbag,
        transform_tree: ecoord::TransformTree,
        start_date_time: Option<DateTime<Utc>>,
        end_date_time: Option<DateTime<Utc>>,
        global_frame_id: &FrameId,
        platform_frame_id: &FrameId,
        slice_duration: chrono::Duration,
        max_points_per_octant: usize,
        campaign_name: String,
        mission_name: String,
        platform_name: String,
        metadata_only: bool,
    ) -> Result<(), Error> {
        // register operations
        let database_platform_entry = self.register_or_get_platform(platform_name).await?;
        let database_campaign_entry = self.register_or_get_campaign(campaign_name).await?;
        let database_mission_entry = self
            .register_or_get_mission(database_campaign_entry.id, mission_name)
            .await?;

        // register recordings of the platform and sensors
        let platform_recording_id = self
            .register_rosbag_platform_recording(
                start_date_time,
                end_date_time,
                database_platform_entry.id,
                database_mission_entry.id,
            )
            .await?;
        let channel_topic_recording_id_mapping: HashMap<ChannelTopic, RecordingId> = self
            .register_rosbag_recordings(
                &rosbag,
                start_date_time,
                end_date_time,
                database_mission_entry.id,
                database_platform_entry.id,
                platform_recording_id,
            )
            .await?;

        // extract relevant channel topics
        let rosbag_overview = rosbag.get_overview()?;
        let relevant_ros_message_types = HashSet::from([
            RosMessageType::SensorMessagesPointCloud2,
            RosMessageType::SensorMessagesImage,
            RosMessageType::Tf2MessagesTFMessage,
        ]);
        let relevant_channel_topics_per_message_type =
            rosbag_overview.get_channel_topics_of_message_types(relevant_ros_message_types);

        let relevant_channel_topics: HashSet<ChannelTopic> =
            relevant_channel_topics_per_message_type
                .values()
                .flatten()
                .cloned()
                .collect();
        let relevant_channel_start_date_time = start_date_time.unwrap_or(
            rosbag
                .get_start_date_time_of_channels(&relevant_channel_topics)?
                .expect("start time of topic should be available"),
        );
        let relevant_channel_end_date_time = end_date_time.unwrap_or(
            rosbag
                .get_end_date_time_of_channels(&relevant_channel_topics)?
                .expect("start time of topic should be available"),
        );

        // import trajectories from transform tree
        let complete_transform_tree = ecoord::merge(&[
            transform_tree.clone(),
            rosbag.get_transforms(&None, &None, &None)?, // TODO: use timestamps
        ])?;
        self.import_trajectory_from_rosbag(
            &rosbag,
            relevant_channel_start_date_time,
            relevant_channel_end_date_time,
            &complete_transform_tree,
            global_frame_id,
            platform_frame_id,
            platform_recording_id,
            &channel_topic_recording_id_mapping,
        )
        .await?;

        // get
        let total_duration = relevant_channel_end_date_time - relevant_channel_start_date_time;
        let num_steps = (total_duration
            .num_nanoseconds()
            .expect("should be derivable") as f64
            / slice_duration
                .num_nanoseconds()
                .expect("should be derivable") as f64)
            .ceil() as i64;
        let time_intervals: Vec<_> = (0..num_steps)
            .map(|step| {
                let start = relevant_channel_start_date_time + slice_duration * step as i32;
                let end = std::cmp::min(start + slice_duration, relevant_channel_end_date_time);
                (start, end)
            })
            .collect();

        info!(
            "🏗️  Importing point clouds from rosbag per time slice with duration of {}",
            humantime::format_duration(slice_duration.to_std()?)
        );
        let progress_bar = get_progress_bar(time_intervals.len() as u64, "time intervals");
        progress_bar.tick();
        for current_time_interval in time_intervals {
            self.import_rosbag_slice(
                &rosbag,
                current_time_interval.0,
                current_time_interval.1,
                global_frame_id,
                &complete_transform_tree,
                max_points_per_octant,
                &channel_topic_recording_id_mapping,
                metadata_only,
            )
            .await?;

            progress_bar.inc(1);
        }
        progress_bar.finish_and_clear();

        Ok(())
    }

    async fn register_rosbag_platform_recording(
        &self,
        start_date_time: Option<DateTime<Utc>>,
        end_date_time: Option<DateTime<Utc>>,
        platform_id: PlatformId,
        mission_id: MissionId,
    ) -> Result<RecordingId, Error> {
        let database_platform_recording_entry = self
            .register_recording(
                None,
                mission_id,
                Some(platform_id),
                None,
                "platform".to_string(),
                start_date_time,
                end_date_time,
            )
            .await?;

        Ok(database_platform_recording_entry.id)
    }

    async fn register_rosbag_recordings(
        &self,
        rosbag: &erosbag::Rosbag,
        start_date_time: Option<DateTime<Utc>>,
        end_date_time: Option<DateTime<Utc>>,
        mission_id: MissionId,
        platform_id: PlatformId,
        platform_recording_id: RecordingId,
    ) -> Result<HashMap<ChannelTopic, RecordingId>, Error> {
        let rosbag_overview = rosbag.get_overview()?;
        let mut channel_topic_recording_id_mapping: HashMap<ChannelTopic, RecordingId> =
            HashMap::new();

        // register lidar sensors
        let relevant_channel_topics = rosbag_overview
            .get_channel_topics_of_message_type(RosMessageType::SensorMessagesPointCloud2);
        for current_channel_topic in relevant_channel_topics.iter().sorted() {
            let current_database_sensor_entry = self
                .register_or_get_sensor(
                    current_channel_topic.to_string(),
                    SensorType::Lidar,
                    platform_id,
                )
                .await?;

            let recording_start_date_time = start_date_time.unwrap_or(
                rosbag
                    .get_start_date_time_of_channel(current_channel_topic)?
                    .expect("start time of topic should be available"),
            );
            let recording_end_date_time = end_date_time.unwrap_or(
                rosbag
                    .get_end_date_time_of_channel(current_channel_topic)?
                    .expect("end time of topic should be available"),
            );
            let current_database_recording_entry = self
                .register_recording(
                    Some(platform_recording_id),
                    mission_id,
                    None,
                    Some(current_database_sensor_entry.id),
                    current_channel_topic.to_string(),
                    Some(recording_start_date_time),
                    Some(recording_end_date_time),
                )
                .await?;

            channel_topic_recording_id_mapping.insert(
                current_channel_topic.clone(),
                current_database_recording_entry.id,
            );
        }

        // register camera sensors
        let relevant_channel_topics =
            rosbag_overview.get_channel_topics_of_message_type(RosMessageType::SensorMessagesImage);
        for current_channel_topic in relevant_channel_topics.iter().sorted() {
            // let current_directory_name = current_channel_topic.to_string().replace("/", "");
            let current_database_sensor_entry = self
                .register_or_get_sensor(
                    current_channel_topic.to_string(),
                    SensorType::Camera,
                    platform_id,
                )
                .await?;

            let recording_start_date_time = start_date_time.unwrap_or(
                rosbag
                    .get_start_date_time_of_channel(current_channel_topic)?
                    .expect("start time of topic should be available"),
            );
            let recording_end_date_time = end_date_time.unwrap_or(
                rosbag
                    .get_end_date_time_of_channel(current_channel_topic)?
                    .expect("end time of topic should be available"),
            );

            let current_database_recording_entry = self
                .register_recording(
                    Some(platform_recording_id),
                    mission_id,
                    None,
                    Some(current_database_sensor_entry.id),
                    current_channel_topic.to_string(),
                    Some(recording_start_date_time),
                    Some(recording_end_date_time),
                )
                .await?;

            channel_topic_recording_id_mapping.insert(
                current_channel_topic.clone(),
                current_database_recording_entry.id,
            );
        }

        Ok(channel_topic_recording_id_mapping)
    }

    async fn import_rosbag_slice(
        &self,
        rosbag: &erosbag::Rosbag,
        start_date_time: DateTime<Utc>,
        end_date_time: DateTime<Utc>,
        global_frame_id: &FrameId,
        complete_transform_tree: &TransformTree,
        max_points_per_octant: usize,
        channel_topic_recording_id_mapping: &HashMap<ChannelTopic, RecordingId>,
        metadata_only: bool,
    ) -> Result<(), Error> {
        let channel_topics: HashSet<ChannelTopic> =
            channel_topic_recording_id_mapping.keys().cloned().collect();
        let rosbag_overview = rosbag.get_overview()?;
        let chunk_ids_to_read: BTreeMap<FileName, Vec<ChunkId>> = rosbag_overview
            .get_chunk_ids_of_channel_topics(
                &Some(start_date_time),
                &Some(end_date_time),
                &channel_topics,
            );

        // read all rosbag chunks of the current time slice
        let pages = chunk_ids_to_read
            .iter()
            .map(|(current_file, current_chunk_ids)| {
                rosbag.get_message_page_with_chunk_ids(
                    current_file,
                    current_chunk_ids,
                    &Some(channel_topics.clone()),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let combined_message_page = McapMessagePage::combine(pages);

        // import sensor data from mcap page
        self.import_point_cloud_from_mcap_page(
            &combined_message_page,
            start_date_time,
            end_date_time,
            global_frame_id,
            complete_transform_tree,
            max_points_per_octant,
            channel_topic_recording_id_mapping,
            metadata_only,
        )
        .await?;

        Ok(())
    }

    async fn import_trajectory_from_rosbag(
        &self,
        rosbag: &erosbag::Rosbag,
        start_date_time: DateTime<Utc>,
        end_date_time: DateTime<Utc>,
        transform_tree: &TransformTree,
        global_frame_id: &FrameId,
        platform_frame_id: &FrameId,
        platform_recording_id: RecordingId,
        channel_topic_recording_id_mapping: &HashMap<ChannelTopic, RecordingId>,
    ) -> Result<(), Error> {
        // register platform trajectory
        if transform_tree.contains_frame(platform_frame_id) {
            let database_trajectory_entry = self
                .register_trajectory(
                    platform_recording_id,
                    TrajectoryDomain::Timed,
                    InterpolationType::Linear,
                    ExtrapolationType::Constant,
                )
                .await?;

            let timed_transform = transform_tree
                .compute_timed_transforms_for_all_samples(&TransformId::new(
                    global_frame_id.clone(),
                    platform_frame_id.clone(),
                ))
                .expect("should work");

            let trajectory_pose_entries: Vec<TrajectoryPoseEntry> = timed_transform
                .into_iter()
                .map(|x| TrajectoryPoseEntry::from_timed_transform(database_trajectory_entry.id, x))
                .collect();

            insert_trajectory_poses(&self.pool, trajectory_pose_entries).await?;
        }

        // lidar and camera sensor trajectories
        let message_page = rosbag.get_message_page_of_first_chunk_per_channel_topic(
            &Some(start_date_time),
            &Some(end_date_time),
            &channel_topic_recording_id_mapping.keys().cloned().collect(),
        )?;
        let mut sensor_frame_ids_by_channel = message_page.point_cloud_frame_ids_by_channel();
        sensor_frame_ids_by_channel.extend(message_page.image_frame_ids_by_channel());

        for (current_channel_topic, current_frame_ids) in sensor_frame_ids_by_channel.iter() {
            let current_database_recording_id = channel_topic_recording_id_mapping
                .get(current_channel_topic)
                .expect("channel topic not found in mapping");

            let database_trajectory_entry = self
                .register_trajectory(
                    *current_database_recording_id,
                    TrajectoryDomain::Timed,
                    InterpolationType::Linear,
                    ExtrapolationType::Constant,
                )
                .await?;

            if current_frame_ids.len() > 1 {
                warn!(
                    "Multiple frames per channel topic {}. Only using first frame.",
                    current_channel_topic
                );
            }

            let transform_id = TransformId::new(
                global_frame_id.clone(),
                current_frame_ids
                    .first()
                    .expect("should have frame ID")
                    .clone(),
            );
            let timed_transform = transform_tree
                .compute_timed_transforms_for_all_samples(&transform_id)
                .expect("should work");

            let trajectory_pose_entries: Vec<TrajectoryPoseEntry> = timed_transform
                .into_iter()
                .map(|x| TrajectoryPoseEntry::from_timed_transform(database_trajectory_entry.id, x))
                .collect();

            insert_trajectory_poses(&self.pool, trajectory_pose_entries).await?;
        }

        Ok(())
    }

    async fn import_point_cloud_from_mcap_page(
        &self,
        message_page: &McapMessagePage,
        start_date_time: DateTime<Utc>,
        end_date_time: DateTime<Utc>,
        global_frame_id: &FrameId,
        additional_transform_tree: &TransformTree,
        max_points_per_octant: usize,
        channel_topic_recording_id_mapping: &HashMap<ChannelTopic, RecordingId>,
        metadata_only: bool,
    ) -> Result<(), Error> {
        for current_topic in message_page.point_cloud_messages.keys() {
            let current_database_recording_id =
                channel_topic_recording_id_mapping.get(current_topic);

            // info!("Loading point cloud for topic {}", current_topic);
            let individual_point_clouds: Vec<epoint::PointCloud> = message_page
                .get_point_cloud_messages(
                    &Some(start_date_time),
                    &Some(end_date_time),
                    &Some(HashSet::from([current_topic.clone()])),
                )?
                .into_par_iter()
                .map(|x| {
                    let mut point_cloud = x.message;

                    point_cloud
                        .point_data
                        .add_u64_column(
                            ExtendedPointDataColumnType::McapChunkId.as_str(),
                            vec![usize::from(x.chunk_id) as u64; point_cloud.point_data.height()],
                        )
                        .expect("should work");
                    point_cloud
                        .point_data
                        .add_u16_column(
                            ExtendedPointDataColumnType::McapMessageId.as_str(),
                            vec![x.message_id.into(); point_cloud.point_data.height()],
                        )
                        .expect("should work");

                    point_cloud
                })
                .collect();
            let mut merged_point_cloud = epoint::transform::merge(individual_point_clouds)?;

            merged_point_cloud.append_transform_tree(additional_transform_tree.clone())?;
            merged_point_cloud.resolve_to_frame(global_frame_id.clone())?;

            let point_cloud_name: String = format!(
                "{}-{}",
                start_date_time.to_rfc3339(),
                end_date_time.to_rfc3339()
            );
            self.import_point_cloud(
                merged_point_cloud,
                current_database_recording_id.copied(),
                Some(point_cloud_name),
                max_points_per_octant,
                metadata_only,
                None,
            )
            .await?;
        }

        Ok(())
    }
}

impl DatabaseManager {
    pub async fn import_point_cloud_directory(
        &self,
        point_cloud_directory_path: impl AsRef<Path>,
        ecoord_directory_path: &Option<impl AsRef<Path>>,
        max_points_per_octant: usize,
        campaign_name: &str,
        mission_name: &str,
        platform_name: &str,
        sensor_name: &str,
        metadata_only: bool,
        global_frame_id: &FrameId,
        _platform_frame_id: &FrameId,
        sensor_frame_id: &FrameId,
    ) -> Result<(), Error> {
        let file_paths: Vec<PathBuf> = WalkDir::new(&point_cloud_directory_path)
            .sort_by_file_name()
            .into_iter()
            .filter_entry(|e| {
                e.file_name()
                    .to_str()
                    .map(|s| !s.starts_with('.'))
                    .unwrap_or(false)
            })
            .filter_map(|r| r.ok())
            .map(|r| r.into_path())
            .filter(|x| epoint::io::PointCloudFormat::is_supported_point_cloud_format(x))
            .collect();
        //info!("Total {}", file_paths.len());

        let database_platform_entry = self
            .register_or_get_platform(platform_name.to_string())
            .await?;
        let database_sensor_entry = self
            .register_or_get_sensor(
                sensor_name.to_string(),
                SensorType::Lidar,
                database_platform_entry.id,
            )
            .await?;
        let database_campaign_entry = self
            .register_or_get_campaign(campaign_name.to_string())
            .await?;
        let database_mission_entry = self
            .register_or_get_mission(database_campaign_entry.id, mission_name.to_string())
            .await?;

        // progress bars
        let multi_progress = MultiProgress::new();
        let point_cloud_progress_bar =
            multi_progress.add(get_progress_bar(file_paths.len() as u64, "point clouds"));
        let point_cloud_cell_progress_bar = multi_progress.insert_after(
            &point_cloud_progress_bar,
            get_progress_bar(file_paths.len() as u64, "point cloud cells"),
        );

        for current_file_path in file_paths.iter() {
            let mut point_cloud = epoint::io::AutoReader::from_path(current_file_path)?.finish()?;
            if let Some(ecoord_directory_path) = ecoord_directory_path {
                let additional_ecoord_reader = ecoord::io::EcoordReader::from_base_path(
                    ecoord_directory_path,
                    current_file_path.file_stem().unwrap().to_str().unwrap(),
                )?;

                if let Some(ecoord_reader) = additional_ecoord_reader {
                    point_cloud.append_transform_tree(ecoord_reader.finish()?)?;
                }
            }

            point_cloud.resolve_to_frame(global_frame_id.clone())?;
            //point_cloud.add_sensor_poses_from_frame(sensor_frame_id.clone())?;
            // info!("Loaded point cloud with {} points", point_cloud.size());

            let current_file_name: String = current_file_path
                .file_stem()
                .expect("should have a filename")
                .to_str()
                .unwrap()
                .to_string();

            let current_database_recording_entry = self
                .register_recording(
                    None,
                    database_mission_entry.id,
                    None,
                    Some(database_sensor_entry.id),
                    current_file_name,
                    point_cloud.point_data.get_timestamp_min()?,
                    point_cloud.point_data.get_timestamp_max()?,
                )
                .await?;

            if point_cloud.transform_tree.contains_frame(sensor_frame_id) {
                self.import_trajectory_from_transform_tree(
                    current_database_recording_entry.id,
                    &point_cloud.transform_tree,
                    global_frame_id,
                    sensor_frame_id,
                )
                .await?;
            }

            self.import_point_cloud(
                point_cloud,
                Some(current_database_recording_entry.id),
                None,
                max_points_per_octant,
                metadata_only,
                Some(point_cloud_cell_progress_bar.clone()),
            )
            .await?;

            point_cloud_progress_bar.inc(1);
        }
        point_cloud_progress_bar.finish();

        Ok(())
    }

    pub async fn import_point_cloud(
        &self,
        mut point_cloud: PointCloud,
        database_recording_id: Option<RecordingId>,
        point_cloud_name: Option<String>,
        max_points_per_octant: usize,
        metadata_only: bool,
        point_cloud_cell_progress_bar: Option<ProgressBar>,
    ) -> Result<(), Error> {
        point_cloud
            .point_data
            .add_sequential_id()
            .expect("should work");
        let recording_start_date_time = point_cloud.point_data.get_timestamp_min()?;
        let recording_end_date_time = point_cloud.point_data.get_timestamp_max()?;

        let current_database_point_cloud_entry = self
            .register_point_cloud(
                database_recording_id,
                point_cloud_name,
                recording_start_date_time,
                recording_end_date_time,
            )
            .await?;

        let point_cloud_octree = PointCloudOctree::new(
            point_cloud,
            max_points_per_octant,
            StorageMode::LeafOctantsOnly,
            None,
        )?;
        if let Some(point_cloud_cell_progress_bar) = point_cloud_cell_progress_bar.clone() {
            point_cloud_cell_progress_bar.reset();
            point_cloud_cell_progress_bar
                .set_length(point_cloud_octree.cell_indices().len() as u64);
        }

        let semaphore = Arc::new(Semaphore::new(self.semaphore_permits));
        let mut handles: Vec<JoinHandle<()>> = vec![];
        for current_cell_index in point_cloud_octree
            .octree
            .cell_indices()
            .into_iter()
            .sorted()
        {
            let future_pool = self.pool.clone();
            let future_semaphore = semaphore.clone();
            let future_point_cloud_cell_progress_bar = point_cloud_cell_progress_bar.clone();

            let current_point_cloud = point_cloud_octree.extract_octant(current_cell_index)?;
            let current_cell_bounding_cube = point_cloud_octree
                .octree
                .bounds()
                .get_octant_bounding_cube(current_cell_index);

            /*info!(
                "Importing point cloud with {} points in the ID range: {}-{}",
                current_point_cloud.size(),
                current_id,
                current_id_max
            );*/
            let current_handle = tokio::spawn(async move {
                let _permit = future_semaphore
                    .acquire()
                    .await
                    .expect("semaphore should not be closed");

                insert_point_cloud_cell(
                    &future_pool,
                    current_database_point_cloud_entry.id,
                    current_cell_index,
                    current_cell_bounding_cube,
                    &current_point_cloud,
                    metadata_only,
                )
                .await
                .expect("should generate insert sql statement");

                if let Some(progress_bar) = future_point_cloud_cell_progress_bar {
                    progress_bar.inc(1);
                }
            });
            handles.push(current_handle);
        }

        for current_handle in handles {
            current_handle.await.unwrap();
        }
        if let Some(progress_bar) = point_cloud_cell_progress_bar.clone() {
            progress_bar.finish();
        }

        Ok(())
    }

    async fn import_trajectory_from_transform_tree(
        &self,
        database_recording_id: RecordingId,
        transform_tree: &TransformTree,
        global_frame_id: &FrameId,
        trajectory_child_frame_id: &FrameId,
    ) -> Result<(), Error> {
        debug_assert!(transform_tree.contains_frame(global_frame_id));

        let trajectory_transform_id =
            TransformId::new(global_frame_id.clone(), trajectory_child_frame_id.clone());

        let trajectory_pose_entries = if transform_tree
            .is_transform_path_static(&trajectory_transform_id)?
        {
            let database_trajectory_entry = self
                .register_trajectory(
                    database_recording_id,
                    TrajectoryDomain::Sequence,
                    InterpolationType::Step,
                    ExtrapolationType::Constant,
                )
                .await?;

            let transform = transform_tree.get_static_transform(&trajectory_transform_id)?;
            let trajectory_pose =
                TrajectoryPoseEntry::from_transform(database_trajectory_entry.id, 0, transform);
            vec![trajectory_pose]
        } else {
            let database_trajectory_entry = self
                .register_trajectory(
                    database_recording_id,
                    TrajectoryDomain::Timed,
                    InterpolationType::Linear,
                    ExtrapolationType::Constant,
                )
                .await?;

            let timed_transforms = transform_tree
                .compute_timed_transforms_for_all_samples(&trajectory_transform_id)?;

            timed_transforms
                .into_iter()
                .map(|x| TrajectoryPoseEntry::from_timed_transform(database_trajectory_entry.id, x))
                .collect()
        };

        insert_trajectory_poses(&self.pool, trajectory_pose_entries).await?;

        Ok(())
    }
}
