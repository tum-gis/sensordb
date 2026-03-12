use crate::database::datatype::PointCloudCellId;
use crate::database::queries::generate_sensor_views::{compute_beams, compute_sensor_poses};
use crate::database::util::get_progress_bar;
use crate::{DatabaseManager, Error};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tracing::info;

impl DatabaseManager {
    pub async fn generate_sensor_views(
        &self,
        reflection_uncertainty_line_length: f32,
    ) -> Result<(), Error> {
        let point_cloud_cell_ids: Vec<PointCloudCellId> =
            sqlx::query_scalar("SELECT id FROM sensordb.point_cloud_cell")
                .fetch_all(&self.pool)
                .await?;

        info!("[1/2] 📍🔦  Compute sensor poses");
        let progress_bar = get_progress_bar(point_cloud_cell_ids.len() as u64, "octree cells");
        let semaphore = Arc::new(Semaphore::new(self.semaphore_permits));
        let mut handles: Vec<JoinHandle<()>> = vec![];
        for current_point_cloud_cell_id in point_cloud_cell_ids.iter().copied() {
            let future_progress_bar = progress_bar.clone();
            let future_pool = self.pool.clone();
            let future_semaphore = semaphore.clone();

            let current_handle = tokio::spawn(async move {
                let _permit = future_semaphore
                    .acquire()
                    .await
                    .expect("semaphore should not be closed");

                compute_sensor_poses(&future_pool, current_point_cloud_cell_id)
                    .await
                    .expect("should work");

                future_progress_bar.inc(1);
            });
            handles.push(current_handle);
        }
        for current_handle in handles {
            current_handle.await.unwrap();
        }
        progress_bar.finish();

        info!("[2/2] 🎯🔦  Compute beams");
        let progress_bar = get_progress_bar(point_cloud_cell_ids.len() as u64, "octree cells");
        let semaphore = Arc::new(Semaphore::new(self.semaphore_permits));
        let mut handles: Vec<JoinHandle<()>> = vec![];
        for current_point_cloud_cell_id in point_cloud_cell_ids.iter().copied() {
            let future_progress_bar = progress_bar.clone();
            let future_pool = self.pool.clone();
            let future_semaphore = semaphore.clone();

            let current_handle = tokio::spawn(async move {
                let _permit = future_semaphore
                    .acquire()
                    .await
                    .expect("semaphore should not be closed");

                compute_beams(
                    &future_pool,
                    current_point_cloud_cell_id,
                    reflection_uncertainty_line_length,
                )
                .await
                .expect("should work");

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
