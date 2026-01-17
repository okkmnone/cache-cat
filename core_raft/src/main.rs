use core_raft::network::model::{Request, Response};
use openraft::Config;
use std::io::Cursor;
use std::sync::Arc;

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub TypeConfig:
        D = Request,
        R = Response,
);
pub type LogStore = core_raft::store::log::LogStore<TypeConfig>;
pub type stateMachineStore = core_raft::store::state_machine::StateMachineStore<TypeConfig>;
fn main() {
    let node_id = 1;
    let config = Arc::new(Config::default().validate().unwrap());
    let log_store = LogStore::default();

}
