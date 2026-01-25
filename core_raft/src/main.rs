use core_raft::network;
use core_raft::network::model::{Request, Response};
use core_raft::network::network::NetworkFactory;
use openraft::Config;
use std::io::Cursor;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    network::raft::main(1, String::from("127.0.0.1:3001")).await
}
