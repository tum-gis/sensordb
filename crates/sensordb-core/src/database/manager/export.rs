use crate::database::datatype::{
    CampaignId, MissionId, PointCloudCellId, PointCloudId, RecordingId, SensorId,
};
use crate::database::queries::export::export_point_cloud_cell;
use crate::database::queries::export::export_point_cloud_for_associated_feature;
use crate::database::queries::export_signatures::export_signatures;
use crate::database::tables::{
    PointCloudAttributeContext, PointCloudAttributeContextEntry, SensorEntry,
};
use crate::database::util::get_progress_bar;
use crate::io::sensors::SensorsDocument;
use crate::{DatabaseManager, Error};
use ecoord::TransformTree;
use epoint::io::{AutoWriter, PointCloudFormat};
use epoint::{PointCloudInfo, PointDataColumnType};
use indicatif::{MultiProgress, ProgressBar};
use itertools::Itertools;
use polars::prelude::{
    IntoLazy, LazyFrame, NamedFrom, ParquetWriter, SerWriter, SortMultipleOptions, concat,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tracing::info;

impl DatabaseManager {
    pub async fn export_sensor_info(&self, directory_path: impl AsRef<Path>) -> Result<(), Error> {
        let sensor_infos: Vec<SensorEntry> =
            sqlx::query_as::<_, SensorEntry>("SELECT * FROM sensordb.sensor")
                .fetch_all(&self.pool)
                .await?;
        if !sensor_infos.is_empty() {
            let sensors_document = SensorsDocument::from(sensor_infos);
            let sensors_document_json = serde_json::to_string_pretty(&sensors_document)?;
            let path = directory_path
                .as_ref()
                .to_owned()
                .clone()
                .join("sensors.json");
            std::fs::write(path, sensors_document_json)?;
        }

        Ok(())
    }

    pub async fn export_signatures(&self, output_path: impl AsRef<Path>) -> Result<(), Error> {
        let mut df = export_signatures(&self.pool).await?;

        let mut file = std::fs::File::create(&output_path)?;
        ParquetWriter::new(&mut file)
            .finish(&mut df)
            .expect("TODO: panic message");
        /*CsvWriter::new(&mut file)
        .include_header(true)
        .with_separator(b';')
        .finish(&mut df)?;*/
        info!(
            "Parquet file with {} signature entries was successfully written to {}",
            df.height(),
            PathBuf::from(output_path.as_ref()).display()
        );

        Ok(())
    }

    pub async fn export(
        &self,
        directory_path: impl AsRef<Path>,
        point_cloud_format: PointCloudFormat,
    ) -> Result<(), Error> {
        let point_cloud_export_contexts: Vec<PointCloudExportContext> = sqlx::query_as!(
            PointCloudExportContext,
            r#"
SELECT
    c.id AS campaign_id, c.name AS campaign_name,
    m.id AS mission_id, m.name AS mission_name,
    r.id AS recording_id, r.name AS recording_name,
    s.id AS sensor_id, s.name AS sensor_name,
    pc.id AS point_cloud_id, pc.name AS point_cloud_name
FROM sensordb.point_cloud pc
JOIN sensordb.recording r on pc.recording_id = r.id
JOIN sensordb.sensor s on r.sensor_id = s.id
JOIN sensordb.mission m on r.mission_id = m.id
JOIN sensordb.campaign c on m.campaign_id = c.id
ORDER BY c.name, m.name, r.name, pc.name;
    "#
        )
        .fetch_all(&self.pool)
        .await?;

        info!("[2/2] ⬇️️☁️ Exporting point clouds");
        // progress bars
        let multi_progress = MultiProgress::new();
        let point_cloud_progress_bar = multi_progress.add(get_progress_bar(
            point_cloud_export_contexts.len() as u64,
            "point clouds",
        ));
        let point_cloud_cell_progress_bar = multi_progress.insert_after(
            &point_cloud_progress_bar,
            get_progress_bar(0_u64, "point cloud cells"),
        );

        for current_point_cloud_export_context in point_cloud_export_contexts.into_iter() {
            let mut point_cloud_path = directory_path
                .as_ref()
                .join(current_point_cloud_export_context.sanitized_campaign_name())
                .join(current_point_cloud_export_context.sanitized_mission_name())
                .join(current_point_cloud_export_context.sanitized_sensor_name())
                .join(current_point_cloud_export_context.sanitized_recording_name());
            if let Some(point_cloud_name) =
                current_point_cloud_export_context.sanitized_point_cloud_name()
            {
                point_cloud_path = point_cloud_path.join(point_cloud_name);
            }
            std::fs::create_dir_all(
                point_cloud_path
                    .parent()
                    .expect("the directory path must have a parent folder"),
            )?;

            self.export_point_cloud(
                current_point_cloud_export_context.point_cloud_id,
                point_cloud_path,
                point_cloud_format,
                Some(point_cloud_cell_progress_bar.clone()),
            )
            .await?;

            point_cloud_progress_bar.inc(1);
        }
        point_cloud_progress_bar.finish();

        Ok(())
    }

    pub async fn export_point_cloud(
        &self,
        point_cloud_id: PointCloudId,
        directory_path: impl AsRef<Path>,
        point_cloud_format: PointCloudFormat,
        point_cloud_cell_progress_bar: Option<ProgressBar>,
    ) -> Result<(), Error> {
        let point_cloud_id: i32 = point_cloud_id.into();
        let point_cloud_cell_ids: Vec<PointCloudCellId> = sqlx::query_scalar!(
            r#"
SELECT id
FROM sensordb.point_cloud_cell
WHERE point_cloud_id = $1;
    "#,
            point_cloud_id
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|id| id.into())
        .collect();

        point_cloud_cell_progress_bar.clone().unwrap().reset();
        point_cloud_cell_progress_bar
            .clone()
            .unwrap()
            .set_length(point_cloud_cell_ids.len() as u64);

        //info!("Exporting point cloud cell id {:?}", point_cloud_cell_ids);

        let point_cloud_attribute_context_entries: Vec<PointCloudAttributeContextEntry> =
            sqlx::query_as!(
                PointCloudAttributeContextEntry,
                r#"
SELECT name AS "name!",
       datatype_id AS "datatype_id!",
       namespace_id AS "namespace_id!",
       is_consistent AS "is_consistent!"
FROM sensordb_pkg.get_point_cloud_attributes($1);
    "#,
                point_cloud_id
            )
            .fetch_all(&self.pool)
            .await?;
        let point_cloud_attribute_context =
            PointCloudAttributeContext::from(point_cloud_attribute_context_entries);

        /*info!(
            "point_cloud_attribute_contexts {:?}",
            point_cloud_attribute_contexts
        );*/

        let semaphore = Arc::new(Semaphore::new(self.semaphore_permits));
        let mut handles: Vec<JoinHandle<Result<LazyFrame, Error>>> = vec![];
        for current_point_cloud_cell_id in point_cloud_cell_ids.into_iter() {
            let future_pool = self.pool.clone();
            let future_semaphore = semaphore.clone();
            let future_point_cloud_attribute_context = point_cloud_attribute_context.clone();
            let future_point_cloud_cell_progress_bar = point_cloud_cell_progress_bar.clone();

            let current_handle = tokio::spawn(async move {
                let _permit = future_semaphore
                    .acquire()
                    .await
                    .expect("semaphore should not be closed");

                let current_dataframe = export_point_cloud_cell(
                    &future_pool,
                    current_point_cloud_cell_id,
                    &future_point_cloud_attribute_context,
                )
                .await?
                .lazy();

                if let Some(progress_bar) = future_point_cloud_cell_progress_bar {
                    progress_bar.inc(1);
                }

                Ok(current_dataframe)
            });
            handles.push(current_handle);
        }

        let mut point_cloud_cell_dataframes: Vec<LazyFrame> = Vec::new();
        for current_handle in handles {
            let dataframe = current_handle.await.unwrap()?;
            point_cloud_cell_dataframes.push(dataframe);
        }
        if let Some(progress_bar) = point_cloud_cell_progress_bar {
            progress_bar.finish();
        }

        let mut combined_dataframe =
            concat(point_cloud_cell_dataframes, Default::default())?.collect()?;
        combined_dataframe = combined_dataframe.sort(
            [PointDataColumnType::Id.as_str()],
            SortMultipleOptions::default(),
        )?;

        let point_cloud_info = PointCloudInfo::new(None);
        let point_cloud = epoint::PointCloud::from_data_frame(
            combined_dataframe,
            point_cloud_info,
            TransformTree::default(),
        )?;

        epoint::io::AutoWriter::from_base_path_with_format(directory_path, point_cloud_format)?
            .finish(point_cloud)?;

        Ok(())
    }

    /*pub async fn export_all_point_cloud_packages(
        &self,
        directory_path: impl AsRef<Path>,
        point_cloud_format: PointCloudFormat,
    ) -> Result<(), Error> {
        let point_cloud_cell_entries: Vec<PointCloudCellEntry> = sqlx::query_as!(
            PointCloudCellEntry,
            r#"
    SELECT id, recording_id, level, x, y, z
    FROM sensordb.point_cloud_cell
    "#
        )
        .fetch_all(&self.pool)
        .await?;
        let packages_named = point_cloud_cell_entries.iter().all(|x| x.name.is_some());
        let progress_bar = get_progress_bar(
            point_cloud_cell_entries.len() as u64,
            "[3/3] ⬇️️☁️ Exporting point cloud packages",
        );

        for current_point_cloud_cell_entry in point_cloud_cell_entries {
            progress_bar.inc(1);
            let point_cloud =
                export_point_cloud_package(&self.pool, current_point_cloud_cell_entry.id).await?;

            let file_name: String = if packages_named {
                if let Some(name) = &current_point_cloud_cell_entry.name {
                    name.clone()
                } else {
                    current_point_cloud_cell_entry.id.to_string()
                }
            } else {
                current_point_cloud_cell_entry.id.to_string()
            };

            let path = directory_path.as_ref().to_owned().clone().join(file_name);
            AutoWriter::from_base_path_with_format(path, point_cloud_format)?
                .finish(point_cloud)?;
        }
        progress_bar.finish();

        Ok(())
    }*/

    /*pub async fn export_all_point_cloud_time_per_recording(
        &self,
        directory_path: impl AsRef<Path>,
        point_cloud_format: PointCloudFormat,
    ) -> Result<(), Error> {
        let campaign_ids = sqlx::query_as::<_, CampaignEntry>("SELECT * FROM sensordb.campaign")
            .fetch_all(&self.pool)
            .await?;

        let campaigns_path = directory_path.as_ref().to_owned().join("campaigns");
        std::fs::create_dir_all(&campaigns_path)?;
        let progress_bar = get_progress_bar(
            campaign_ids.len() as u64,
            "[3/3] ⬇️️☁️ Exporting point cloud recordings",
        );

        for current_campaign in campaign_ids {
            let current_campaign_path = campaigns_path.join(&current_campaign.name);
            std::fs::create_dir_all(&current_campaign_path)?;

            self.export_campaign_point_cloud_recordings(
                current_campaign.id,
                current_campaign_path,
                point_cloud_format,
            )
            .await?;
            progress_bar.inc(1);
        }
        progress_bar.finish();

        Ok(())
    }*/

    /*pub async fn export_campaign_point_cloud_recordings(
        &self,
        campaign_id: CampaignId,
        directory_path: impl AsRef<Path>,
        point_cloud_format: PointCloudFormat,
    ) -> Result<(), Error> {
        let recording_entries = sqlx::query_as::<_, RecordingEntry>(
            "SELECT *
FROM sensordb.recording
JOIN sensordb.mission ON mission_id = sensordb.mission.id
WHERE campaign_id = $1",
        )
        .bind(campaign_id)
        .fetch_all(&self.pool)
        .await?;

        let mut handles: Vec<JoinHandle<()>> = vec![];
        for current_recording_entry in recording_entries {
            let file_name = if let Some(name) = current_recording_entry.name {
                name
            } else {
                current_recording_entry.id.to_string()
            };
            let path = directory_path.as_ref().to_owned().clone().join(file_name);
            let point_cloud_cell_ids = sqlx::query_as::<_, (RecordingId,)>(
                "SELECT point_cloud_cell.id
FROM sensordb.point_cloud_cell
JOIN sensordb.point_cloud ON point_cloud_cell.point_cloud_id = point_cloud.id
WHERE recording_id = $1",
            )
            .bind(current_recording_entry.id)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|(id,)| id)
            .collect::<Vec<RecordingId>>();

            let future_pool = self.pool.clone();

            let current_handle = tokio::spawn(async move {
                let point_cloud = export_point_cloud_cell(&future_pool, point_cloud_cell_ids)
                    .await
                    .expect("Exporting point cloud recordings");

                AutoWriter::from_base_path_with_format(path, point_cloud_format)
                    .unwrap()
                    .finish(point_cloud)
                    .unwrap();
            });
            handles.push(current_handle);
        }
        for current_handle in handles {
            current_handle.await.unwrap();
        }

        Ok(())
    }*/

    /*pub async fn export_all_point_cloud_time_slices(
        &self,
        directory_path: impl AsRef<Path>,
        point_cloud_format: PointCloudFormat,
        step_duration: chrono::Duration,
    ) -> Result<(), Error> {
        let campaign_ids = sqlx::query_as::<_, CampaignEntry>("SELECT * FROM sensordb.campaign")
            .fetch_all(&self.pool)
            .await?;

        let campaigns_path = directory_path.as_ref().to_owned().join("campaigns");
        std::fs::create_dir_all(&campaigns_path)?;
        for current_campaign in campaign_ids {
            let current_campaign_path = campaigns_path.join(&current_campaign.name);
            std::fs::create_dir_all(&current_campaign_path)?;

            self.export_campaign_point_cloud_time_slices(
                current_campaign.id,
                current_campaign.name,
                current_campaign_path,
                point_cloud_format,
                step_duration,
            )
            .await?;
        }

        Ok(())
    }*/

    /*pub async fn export_campaign_point_cloud_time_slices(
        &self,
        campaign_id: CampaignId,
        campaign_name: String,
        directory_path: impl AsRef<Path>,
        point_cloud_format: PointCloudFormat,
        step_duration: chrono::Duration,
    ) -> Result<(), Error> {
        info!(
            "Start determining start and end times of campaign {}",
            campaign_name
        );
        let start_date_time = sqlx::query_scalar::<_, Option<i32>>(
            "SELECT MIN(timestamp_sec)
     FROM sensordb.lidar_beam_enriched
     WHERE campaign_id = $1",
        )
        .bind(campaign_id)
        .fetch_one(&self.pool)
        .await?
        .expect("no start time");
        let start_date_time = Utc.timestamp_opt(start_date_time as i64, 0).unwrap();

        let end_date_time = sqlx::query_scalar::<_, Option<i32>>(
            "SELECT MAX(timestamp_sec) FROM sensordb.lidar_beam_enriched WHERE campaign_id = $1",
        )
        .bind(campaign_id)
        .fetch_one(&self.pool)
        .await?
        .expect("no start time");
        let end_date_time = Utc.timestamp_opt(end_date_time as i64, 0).unwrap();

        let total_duration = end_date_time - start_date_time;
        let total_steps: i32 =
            (total_duration.num_milliseconds() / step_duration.num_milliseconds()) as i32;
        let progress_bar = get_progress_bar(
            total_steps as u64,
            &format!("[3/3] ⬇️️☁️ Exporting point cloud time slices for campaign {campaign_name}"),
        );

        let mut handles: Vec<JoinHandle<()>> = vec![];
        for current_step in 0..total_steps {
            let step_start_time = start_date_time + step_duration * current_step;
            let step_end_time = step_start_time + step_duration;

            let file_name = format!("{}", step_start_time.format("%Y-%m-%d_%H-%M-%S.%f"),);
            let path = directory_path.as_ref().to_owned().clone().join(file_name);
            let future_progress_bar = progress_bar.clone();
            let future_pool = self.pool.clone();

            let current_handle = tokio::spawn(async move {
                let point_cloud = export_point_cloud_time_slice(
                    &future_pool,
                    campaign_id,
                    step_start_time,
                    step_end_time,
                )
                .await
                .expect("Exporting point cloud time slices");

                AutoWriter::from_base_path_with_format(path, point_cloud_format)
                    .unwrap()
                    .finish(point_cloud)
                    .unwrap();
                future_progress_bar.inc(1);
            });
            handles.push(current_handle);
        }
        for current_handle in handles {
            current_handle.await.unwrap();
        }
        progress_bar.finish();

        Ok(())
    }*/

    pub async fn export_all_point_cloud_associated_features(
        &self,
        directory_path: impl AsRef<Path>,
        point_cloud_format: PointCloudFormat,
    ) -> Result<(), Error> {
        let feature_object_ids: Vec<String> = sqlx::query_scalar::<_, Option<String>>(
            "SELECT DISTINCT feature_object_id FROM sensordb.lidar_beam_enriched;",
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .flatten()
        .collect();

        let progress_bar = get_progress_bar(
            feature_object_ids.len() as u64,
            "[3/3] ⬇️️☁️ Exporting point cloud per associated feature",
        );

        let point_clouds_path = directory_path.as_ref().to_owned().join("point_clouds");
        std::fs::create_dir_all(&point_clouds_path)?;

        let mut handles: Vec<JoinHandle<()>> = vec![];
        for current_feature_object_id in feature_object_ids.into_iter() {
            let path = point_clouds_path
                .clone()
                .join(current_feature_object_id.clone());
            let future_progress_bar = progress_bar.clone();
            let future_pool = self.pool.clone();

            let current_handle = tokio::spawn(async move {
                let point_cloud = export_point_cloud_for_associated_feature(
                    &future_pool,
                    current_feature_object_id,
                )
                .await
                .expect("should work");

                AutoWriter::from_base_path_with_format(path, point_cloud_format)
                    .unwrap()
                    .finish(point_cloud)
                    .unwrap();
                future_progress_bar.inc(1);
            });
            handles.push(current_handle);
        }
        for current_handle in handles {
            current_handle.await.unwrap();
        }
        progress_bar.finish();

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
struct PointCloudExportContext {
    campaign_name: String,
    campaign_id: CampaignId,
    mission_name: String,
    mission_id: MissionId,
    recording_name: String,
    recording_id: RecordingId,
    sensor_name: String,
    sensor_id: SensorId,
    point_cloud_name: Option<String>,
    point_cloud_id: PointCloudId,
}

impl PointCloudExportContext {
    pub fn sanitized_campaign_name(&self) -> String {
        sanitize_filename::sanitize(&self.campaign_name)
    }

    pub fn sanitized_mission_name(&self) -> String {
        sanitize_filename::sanitize(&self.mission_name)
    }

    pub fn sanitized_recording_name(&self) -> String {
        sanitize_filename::sanitize(&self.recording_name)
    }

    pub fn sanitized_sensor_name(&self) -> String {
        sanitize_filename::sanitize(&self.sensor_name)
    }

    pub fn sanitized_point_cloud_name(&self) -> Option<String> {
        self.point_cloud_name
            .as_ref()
            .map(sanitize_filename::sanitize)
    }
}
