use crate::database::datatype::PointCloudCellId;
use crate::database::queries::association::{associate, explode_feature_geometry_data};
use crate::database::util::get_progress_bar;
use crate::{DatabaseManager, Error};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tracing::info;

impl DatabaseManager {
    pub async fn associate(
        &self,
        reflection_uncertainty_point_buffer: f32,
        reflection_uncertainty_line_buffer: f32,
        max_reflection_uncertainty_line_intersection_parameter: f32,
        _maximum_return_number: Option<i32>,
    ) -> Result<(), Error> {
        info!("[1/3] 🔗🧹  Clean association tables");
        self.clean_associations().await?;

        info!("[2/3] 💥📐  Explode feature geometry data");
        explode_feature_geometry_data(&self.pool).await?;

        let point_cloud_cell_ids: Vec<PointCloudCellId> =
            sqlx::query_scalar("SELECT id FROM sensordb.point_cloud_cell")
                .fetch_all(&self.pool)
                .await?;

        info!(
            "[3/3] 🔦🔗📐 Create associations between sensor observations and feature geometries"
        );
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

                associate(
                    &future_pool,
                    current_point_cloud_cell_id,
                    reflection_uncertainty_point_buffer,
                    reflection_uncertainty_line_buffer,
                    max_reflection_uncertainty_line_intersection_parameter,
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
