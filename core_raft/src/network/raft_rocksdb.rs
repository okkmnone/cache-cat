use crate::network::model::{Request, Response};
use crate::network::network::NetworkFactory;
use crate::network::node::{App, CacheCatApp};
use crate::server::handler::model::SetReq;
use crate::server::handler::rpc;
use crate::store::rocks_store::new_storage;
use openraft::{BasicNode, Config};
use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::path::Path;
use std::sync::{Arc, LazyLock};
use tokio::sync::Mutex;

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub TypeConfig:
        D = Request,
        R = Response,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        NodeId=u16,
);
pub type GroupId = u16;
pub type NodeId = u16;

//实现是纯内存的暂时
pub type LogStore = crate::store::rocks_log_store::RocksLogStore;
pub type StateMachineStore = crate::store::rocks_store::StateMachineStore;
pub type Raft = openraft::Raft<TypeConfig>;
pub async fn start_raft_app<P>(node_id: NodeId, dir: P, addr: String) -> std::io::Result<()>
where
    P: AsRef<Path>,
{
    let config = Arc::new(Config {
        heartbeat_interval: 2500,
        election_timeout_min: 2990,
        election_timeout_max: 5990, // 添加最大选举超时时间
        ..Default::default()
    });

    let (log_store, state_machine_store) = new_storage(&dir).await;
    let network = NetworkFactory {};

    let raft = openraft::Raft::new(
        node_id,
        config.clone(),
        network,
        log_store,
        state_machine_store.clone(),
    )
    .await
    .unwrap();

    let app = CacheCatApp {
        id: node_id,
        addr: addr.clone(),
        raft,
        group_id: 0,
        state_machine: state_machine_store,
    };

    // 正确构建集群成员映射
    let mut nodes = BTreeMap::new();
    if node_id == 3 {
        nodes.insert(
            1,
            BasicNode {
                addr: "127.0.0.1:3001".to_string(),
            },
        );
        nodes.insert(
            2,
            BasicNode {
                addr: "127.0.0.1:3002".to_string(),
            },
        );
        nodes.insert(
            3,
            BasicNode {
                addr: "127.0.0.1:3003".to_string(),
            },
        );
        app.raft.initialize(nodes).await.unwrap();
    }
    // 根据node_id决定完整的集群配置

    rpc::start_server(App::new(vec![app]), addr).await
}
