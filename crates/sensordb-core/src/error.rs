use polars::error::PolarsError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    EcoordError(#[from] ecoord::Error),
    #[error(transparent)]
    EcoordIoError(#[from] ecoord::io::Error),
    #[error(transparent)]
    ErosbagError(#[from] erosbag::Error),
    #[error(transparent)]
    EpointError(#[from] epoint::Error),
    #[error(transparent)]
    EpointIoError(#[from] epoint::io::Error),
    #[error(transparent)]
    EpointTransformError(#[from] epoint::transform::Error),

    #[error(transparent)]
    StdIoError(#[from] std::io::Error),
    #[error(transparent)]
    ChronoOutOfRangeError(#[from] chrono::OutOfRangeError),
    #[error(transparent)]
    SqlxResult(#[from] sqlx::Error),
    #[error(transparent)]
    SqlxMigrateResult(#[from] sqlx::migrate::MigrateError),
    #[error(transparent)]
    PolarsResult(#[from] PolarsError),
    #[error(transparent)]
    SerdeJsonResult(#[from] serde_json::Error),
}
