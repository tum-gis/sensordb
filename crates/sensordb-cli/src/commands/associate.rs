use crate::cli::Connection;
use crate::error::Error;
use sensordb::DatabaseManager;
use std::time::Instant;
use tracing::info;

#[tokio::main]
pub async fn run(
    connection: &Connection,
    reflection_uncertainty_point_buffer: f32,
    reflection_uncertainty_line_buffer: f32,
    max_reflection_uncertainty_line_intersection_parameter: f32,
    maximum_return_number: Option<i32>,
) -> Result<(), Error> {
    info!(
        "Run associate with max_reflection_uncertainty_line_intersection_parameter {max_reflection_uncertainty_line_intersection_parameter} and reflection_uncertainty_line_buffer {reflection_uncertainty_line_buffer}"
    );

    let database_manager = DatabaseManager::new(
        &connection.get_connection_string(),
        connection.db_max_connections,
    )
    .await?;

    let start = Instant::now();
    database_manager
        .associate(
            reflection_uncertainty_point_buffer,
            reflection_uncertainty_line_buffer,
            max_reflection_uncertainty_line_intersection_parameter,
            maximum_return_number,
        )
        .await?;
    let duration = std::time::Duration::from_secs(start.elapsed().as_secs());
    info!(
        "✅  Successful association process took {} with {} connections.",
        humantime::format_duration(duration),
        connection.db_max_connections
    );

    Ok(())
}
