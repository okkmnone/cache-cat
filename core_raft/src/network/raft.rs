use crate::network::model::{Request, Response};
use crate::network::network::NetworkFactory;
use crate::server::handler::rpc;
use futures::lock::Mutex;
use openraft::Config;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub TypeConfig:
        D = Request,
        R = Response,
        Entry = openraft::Entry<TypeConfig>,
);
pub type Raft = openraft::Raft<TypeConfig>;
pub type LogStore = crate::store::log::LogStore;
pub type StateMachineStore = crate::store::state_machine::StateMachineStore<TypeConfig>;
pub async fn main(node_id: u64, addr: String) -> Result<(), Box<dyn std::error::Error>> {
    let node_id = 1;
    let config = Arc::new(Config::default().validate()?);
    let log_store = LogStore::default();
    let state_machine_store = StateMachineStore::default();
    //客户端网络
    let network = NetworkFactory {};
    // Create a local raft instance.
    let raft = openraft::Raft::new(
        node_id,
        config.clone(),
        network,
        log_store,
        state_machine_store.clone(),
    )
    .await?;
    let app = CacheCatApp {
        id: node_id,
        addr: addr.clone(),
        raft,
        config,
    };
    //服务端网络
    rpc::start_server(Arc::new(app)).await?;
    Ok(())
}
pub struct CacheCatApp {
    pub id: u64,
    pub addr: String,
    pub raft: Raft,
    pub config: Arc<Config>,
}
