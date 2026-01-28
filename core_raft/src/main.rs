use core_raft::network;
use core_raft::network::raft_rocksdb::TypeConfig;
use openraft::AsyncRuntime;
use openraft::alias::AsyncRuntimeOf;
use std::{fs, thread};
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = r"E:\tmp\raft\rocks";

    // 确保父目录存在（很重要）
    fs::create_dir_all(base)?;

    let d1 = tempfile::TempDir::new_in(base)?;
    let d2 = tempfile::TempDir::new_in(base)?;
    let d3 = tempfile::TempDir::new_in(base)?;
    // Setup the logger
    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_ansi(false)
        .with_env_filter(EnvFilter::from_default_env())
        .with_max_level(tracing::Level::INFO)
        .init();
    let _h1 = thread::spawn(move || {
        let mut rt = AsyncRuntimeOf::<TypeConfig>::new(1);
        let x = rt.block_on(network::raft_rocksdb::start_raft_app(
            1,
            d1.path(),
            String::from("127.0.0.1:3001"),
        ));
    });
    let _h2 = thread::spawn(move || {
        let mut rt = AsyncRuntimeOf::<TypeConfig>::new(1);
        let x = rt.block_on(network::raft_rocksdb::start_raft_app(
            2,
            d2.path(),
            String::from("127.0.0.1:3002"),
        ));
    });
    let _h3 = thread::spawn(move || {
        let mut rt = AsyncRuntimeOf::<TypeConfig>::new(1);
        let x = rt.block_on(network::raft_rocksdb::start_raft_app(
            3,
            d3.path(),
            String::from("127.0.0.1:3003"),
        ));
    });
    thread::sleep(Duration::from_secs(2));

    thread::sleep(Duration::from_secs(20000));
    Ok(())
    // network::raft::start_raft_app(1, String::from("127.0.0.1:3001")).await
}
