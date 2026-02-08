use crate::network::network::NetworkFactory;
use crate::network::raft_rocksdb::{GroupId, NodeId, Raft, StateMachineStore, TypeConfig};
use crate::network::router::{MultiNetworkFactory, Router};
use crate::store::rocks_store::{StateMachineData, new_storage};
use openraft::Config;
use openraft_multi::GroupNetworkFactory;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::sync::Mutex;

const GROUP_NUM: i16 = 2;

pub struct CacheCatApp {
    pub id: NodeId,
    pub addr: String,
    pub raft: Raft,
    pub group_id: GroupId,
    pub state_machine: StateMachineStore,
}
pub type App = Arc<Vec<CacheCatApp>>;
pub fn get_app(app: &App, group_id: GroupId) -> &CacheCatApp {
    app.iter().find(|app| app.group_id == group_id).unwrap()
}
pub fn get_group(app: &App, hash_code: u64) -> &CacheCatApp {
    let usize = hash_code % app.len() as u64;
    get_app(app, usize as GroupId)
}

pub struct Node {
    pub node_id: NodeId,
    pub groups: HashMap<GroupId, CacheCatApp>,
    pub router: Router,
}
impl Node {
    pub fn new(node_id: NodeId, addr: String) -> Self {
        let router = Router::new(addr.clone());
        Self {
            node_id,
            groups: HashMap::new(),
            router,
        }
    }
    pub fn add_group(
        &mut self,
        addr: &String,
        group_id: GroupId,
        raft: Raft,
        state_machine: StateMachineStore,
    ) {
        let app = CacheCatApp {
            id: self.node_id,
            addr: addr.clone(),
            raft,
            group_id,
            state_machine,
        };
        self.groups.insert(group_id, app);
    }
}

pub async fn create_node<P>(addr: &String, node_id: NodeId, dir: P) -> Node
where
    P: AsRef<Path>,
{
    let mut node = Node::new(node_id, addr.to_string());
    for i in 0..GROUP_NUM {
        let group_id = i as GroupId;
        let group_dir: PathBuf = Path::new(dir.as_ref()).join(i.to_string());
        fs::create_dir_all(&group_dir).await.unwrap();
        let config = Arc::new(Config {
            heartbeat_interval: 2500,
            election_timeout_min: 2990,
            election_timeout_max: 5990, // 添加最大选举超时时间
            ..Default::default()
        });
        let router = Router::new(addr.to_string());
        let network = MultiNetworkFactory::new(router.clone(), group_id);
        let (log_store, state_machine_store) = new_storage(&group_dir).await;
        let raft = openraft::Raft::new(
            node_id,
            config.clone(),
            network,
            log_store,
            state_machine_store.clone(),
        )
        .await
        .unwrap();
        node.add_group(addr, group_id, raft, state_machine_store)
    }
    node
}
