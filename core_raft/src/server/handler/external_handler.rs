use crate::error::{CoreRaftError, CoreRaftResult};
use crate::network::model::Request;
use crate::network::node::{App, TypeConfig, get_app, get_group};
use crate::server::handler::model::*;
use async_trait::async_trait;
use bytes::Bytes;
use openraft::Snapshot;
use openraft::error::{ClientWriteError, Fatal, RPCError, RaftError, RemoteError};
use openraft::raft::{
    AppendEntriesResponse, ClientWriteResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    SnapshotResponse, VoteRequest, VoteResponse,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::Instant;
use std::{fmt, io};
use std::sync::LazyLock;

static HANDLER_TABLE: LazyLock<[Option<Box<dyn RpcHandler>>; 128]> = LazyLock::new(|| {
    std::array::from_fn(|i|
        match i {
            1 => Some(Box::new(RpcMethod { func: print_test }) as _),
            2 => Some(Box::new(RpcMethod { func: write }) as _),
            3 => Some(Box::new(RpcMethod { func: read }) as _),
            6 => Some(Box::new(RpcMethod { func: vote }) as _),
            7 => Some(Box::new(RpcMethod { func: append_entries }) as _),
            8 => Some(Box::new(RpcMethod { func: install_full_snapshot }) as _),
            _ => None,
        }
    )
});

#[inline]
pub fn get_handler(func_id: usize) -> Option<&'static dyn RpcHandler> {
    HANDLER_TABLE.get(func_id)?.as_ref().map(|boxed| boxed.as_ref())
}

fn hash_string(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

#[async_trait]
pub trait RpcHandler: Send + Sync {
    // 将 app 改为 Arc 传递，更符合异步环境下的生命周期要求
    async fn internal_call(&self, app: App, data: Bytes) -> Bytes;
}

// 修改函数指针定义，使其支持异步返回 Future
// 这里使用泛型 F 来适配异步函数
pub struct RpcMethod<Req, Res, Fut>
where
    Fut: Future<Output = CoreRaftResult<Res>>,
{
    // 注意：Rust 的纯函数指针 fn 不能直接是 async 的
    // 我们这里让 func 返回一个 Future
    func: fn(App, Req) -> Fut,
}

#[async_trait]
impl<Req, Res, Fut> RpcHandler for RpcMethod<Req, Res, Fut>
where
    Req: DeserializeOwned,
    Res: Serialize,
    Fut: Future<Output = CoreRaftResult<Res>> + Send,
{
    async fn internal_call(&self, app: App, data: Bytes) -> Bytes {
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

async fn print_test(_app: App, d: PrintTestReq) -> CoreRaftResult<PrintTestRes> {
    // Ok(PrintTestRes { message: d.message })
    Err(CoreRaftError::StdIoError(io::Error::new(
        io::ErrorKind::Other,
        "unsupported version",
    )))
}

// 主节点才能成功调用这个方法，其他节点会失败
async fn write(app: App, req: Request) -> CoreRaftResult<ClientWriteResponse<TypeConfig>> {
    // 根据请求判断属于哪个组
    let group = get_group(&app, req.hash_code());
    let res = group.raft.client_write(req).await?;
    Ok(res)
}
async fn read(app: App, req: String) -> CoreRaftResult<Option<String>> {
    // let group = get_group(&app, hash_string(&req));
    // let kvs = group.state_machine.data.kvs.lock().await;
    // let value = kvs.get(&req);
    // value.map(|v| String::from_utf8(v.tostring()))
    todo!()
}

//TODO 向上传播错误
async fn vote(app: App, req: VoteReq) -> CoreRaftResult<VoteResponse<TypeConfig>> {
    // openraft 的 vote 是异步的
    let group = get_app(&app, req.group_id);
    Ok(group.raft.vote(req.vote).await?)
}

//理论上只有从节点会被调用这个方法
async fn append_entries(
    app: App,
    req: AppendEntriesReq,
) -> CoreRaftResult<AppendEntriesResponse<TypeConfig>> {
    let start = Instant::now();
    let e = req.append_entries.entries.is_empty();
    let res = get_app(&app, req.group_id)
        .raft
        .append_entries(req.append_entries)
        .await?;
    let elapsed = start.elapsed();
    if !e {
        tracing::info!("append 从节点内部处理: {:?} ", elapsed);
    }
    Ok(res)
}

//InstallFullSnapshotReq 把openraft自带的俩个参数包裹在一起了
// 从节点收到数据 在这里序列化到磁盘 后续install_full_snapshot会从磁盘中反序列化
async fn install_full_snapshot(
    app: App,
    req: InstallFullSnapshotReq,
) -> CoreRaftResult<SnapshotResponse<TypeConfig>> {
    let snapshot = Snapshot {
        meta: req.snapshot_meta,
        snapshot: req.snapshot,
    };
    let res = get_app(&app, req.group_id)
        .raft
        .install_full_snapshot(req.vote, snapshot)
        .await?;
    Ok(res)
}
