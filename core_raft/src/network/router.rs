use crate::network::network::{NetworkFactory, TcpNetwork};
use crate::network::raft_rocksdb::{GroupId, NodeId, TypeConfig};
use openraft::alias::VoteOf;
use openraft::error::{RPCError, ReplicationClosed, StreamingError};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{OptionalSend, RaftNetworkV2, Snapshot};
use openraft_multi::GroupRouter;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Multi-Raft Router with per-node connection sharing.
#[derive(Clone, Default)]
pub struct Router {
    /// Map from node_id to node connection.
    /// All groups on the same node share this connection.
    pub nodes: Arc<RwLock<HashMap<NodeId, TcpNetwork>>>,
    pub addr: String,
}
impl Router {
    pub fn new(addr: String) -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            addr,
        }
    }
    pub async fn register_node(&mut self, node_id: NodeId) {
        let net = NetworkFactory::new_tcp(node_id, self.addr.to_string()).await;
        self.nodes.write().await.insert(node_id, net);
    }
}
impl GroupRouter<TypeConfig, GroupId> for Router {
    async fn append_entries(
        &self,
        target: NodeId,
        group_id: GroupId,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        let x = self.nodes.read().await.get(&target).unwrap();
        todo!()
    }

    async fn vote(
        &self,
        target: NodeId,
        group_id: GroupId,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        todo!()
    }

    async fn full_snapshot(
        &self,
        target: NodeId,
        group_id: GroupId,
        vote: VoteOf<TypeConfig>,
        snapshot: Snapshot<TypeConfig>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        todo!()
    }
}
