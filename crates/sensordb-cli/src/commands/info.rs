use crate::cli::Connection;
use crate::error::Error;

#[tokio::main]
pub async fn run(_connection: &Connection) -> anyhow::Result<(), Error> {
    // Get Tokio runtime thread pool size
    let tokio_threads = tokio::runtime::Handle::current().metrics().num_workers();
    println!("Tokio thread pool size: {}", tokio_threads);

    // Get Rayon thread pool size
    let rayon_threads = rayon::current_num_threads();
    println!("Rayon thread pool size: {}", rayon_threads);

    Ok(())
}
