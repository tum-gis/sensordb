use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    SensordbError(#[from] sensordb::Error),

    #[error(transparent)]
    ErosbagError(#[from] erosbag::Error),
    #[error(transparent)]
    ErosbagTransformError(#[from] erosbag::transform::Error),
    #[error(transparent)]
    EcoordError(#[from] ecoord::Error),
    #[error(transparent)]
    EcoordIoError(#[from] ecoord::io::Error),
    #[error(transparent)]
    EpointError(#[from] epoint::Error),
    #[error(transparent)]
    EpointIoError(#[from] epoint::io::Error),

    #[error(transparent)]
    StdIoError(#[from] std::io::Error),
    #[error(transparent)]
    ChronoOutOfRangeError(#[from] chrono::OutOfRangeError),
}
