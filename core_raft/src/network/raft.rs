use crate::network::model::{Request, Response};
use crate::network::network::NetworkFactory;
use crate::server::handler::rpc;
use openraft::{BasicNode, Config};
use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::sync::{Arc, LazyLock};

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub TypeConfig:
        D = Request,
        R = Response,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
);
pub type Raft = openraft::Raft<TypeConfig>;
//实现是纯内存的暂时
pub type LogStore = crate::store::log::LogStore;
pub type StateMachineStore = crate::store::state_machine::StateMachineStore<TypeConfig>;

static MAP: LazyLock<HashMap<u64, Vec<String>>> = LazyLock::new(|| {
    let mut map: HashMap<u64, Vec<String>> = HashMap::new();
    map.insert(
        1,
        vec!["127.0.0.1:3002".to_string(), "127.0.0.1:3003".to_string()],
    );
    map.insert(
        2,
        vec!["127.0.0.1:3001".to_string(), "127.0.0.1:3003".to_string()],
    );
    map.insert(
        3,
        vec!["127.0.0.1:3001".to_string(), "127.0.0.1:3002".to_string()],
    );
    return map;
});
pub async fn start_raft_app(node_id: u64, addr: String) -> std::io::Result<()> {
    let config = Arc::new(Config {
        heartbeat_interval: 250,
        election_timeout_min: 299,
        election_timeout_max: 599, // 添加最大选举超时时间
        ..Default::default()
    });

    let log_store = LogStore::default();
    let state_machine_store = StateMachineStore::default();
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
        config,
    };

    // 正确构建集群成员映射
    let mut nodes = BTreeMap::new();

    // 根据node_id决定完整的集群配置
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

    rpc::start_server(Arc::new(app)).await
}
pub struct CacheCatApp {
    pub id: u64,
    pub addr: String,
    pub raft: Raft,
    pub config: Arc<Config>,
}
