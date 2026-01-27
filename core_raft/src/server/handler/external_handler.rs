use crate::network::raft::{CacheCatApp, TypeConfig};
use crate::server::handler::model::{
    DelReq, DelRes, ExistsReq, ExistsRes, GetReq, GetRes, InstallFullSnapshotReq, PrintTestReq,
    PrintTestRes, SetReq, SetRes,
};
use async_trait::async_trait;
use bytes::Bytes;
use openraft::Snapshot;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    SnapshotResponse, VoteRequest, VoteResponse,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::io::Cursor;
use std::sync::Arc;

pub type HandlerEntry = (u32, fn() -> Box<dyn RpcHandler>);

pub static HANDLER_TABLE: &[HandlerEntry] = &[
    (1, || Box::new(RpcMethod { func: print_test })),
    (6, || Box::new(RpcMethod { func: vote })),
    (7, || {
        Box::new(RpcMethod {
            func: append_entries,
        })
    }),
    (8, || {
        Box::new(RpcMethod {
            func: install_full_snapshot,
        })
    }),
];

#[async_trait]
pub trait RpcHandler: Send + Sync {
    // 将 app 改为 Arc 传递，更符合异步环境下的生命周期要求
    async fn call(&self, app: Arc<CacheCatApp>, data: Bytes) -> Bytes;
}

// 修改函数指针定义，使其支持异步返回 Future
// 这里使用泛型 F 来适配异步函数
pub struct RpcMethod<Req, Res, Fut>
where
    Fut: std::future::Future<Output = Res> + Send,
{
    // 注意：Rust 的纯函数指针 fn 不能直接是 async 的
    // 我们这里让 func 返回一个 Future
    func: fn(Arc<CacheCatApp>, Req) -> Fut,
}

#[async_trait]
impl<Req, Res, Fut> RpcHandler for RpcMethod<Req, Res, Fut>
where
    Req: Send + 'static + DeserializeOwned,
    Res: Send + 'static + Serialize,
    Fut: std::future::Future<Output = Res> + Send + 'static,
{
    async fn call(&self, app: Arc<CacheCatApp>, data: Bytes) -> Bytes {
        // 反序列化
        let req: Req = bincode2::deserialize(data.as_ref()).expect("Failed to deserialize");
        // 执行异步业务函数
        let res = (self.func)(app, req).await;
        // 序列化
        let encoded: Vec<u8> = bincode2::serialize(&res).expect("Failed to serialize");
        encoded.into()
    }
}

// --- 业务函数全部改为 async ---

async fn print_test(_app: Arc<CacheCatApp>, d: PrintTestReq) -> PrintTestRes {
    println!("{}", d.message);
    PrintTestRes { message: d.message }
}

async fn vote(app: Arc<CacheCatApp>, req: VoteRequest<TypeConfig>) -> VoteResponse<TypeConfig> {
    // openraft 的 vote 是异步的
    app.raft.vote(req).await.expect("Raft vote failed")
}
async fn append_entries(
    app: Arc<CacheCatApp>,
    req: AppendEntriesRequest<TypeConfig>,
) -> AppendEntriesResponse<TypeConfig> {
    app.raft
        .append_entries(req)
        .await
        .expect("Raft append_entries failed")
}

//InstallFullSnapshotReq 把openraft自带的俩个参数包裹在一起了
async fn install_full_snapshot(
    app: Arc<CacheCatApp>,
    req: InstallFullSnapshotReq,
) -> SnapshotResponse<TypeConfig> {
    let mut snapshot_data_bytes: Vec<u8> = Vec::new();
    let sp = Cursor::new(snapshot_data_bytes);
    let snapshot = Snapshot {
        meta: req.snapshot_meta,
        snapshot: sp,
    };
    app.raft
        .install_full_snapshot(req.vote, snapshot)
        .await
        .expect("Raft install_snapshot failed")
}
