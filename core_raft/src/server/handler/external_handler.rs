use crate::network::raft::{CacheCatApp, TypeConfig};
use crate::server::core::moka::{MyValue, get_cache};
use crate::server::handler::model::{
    DelReq, DelRes, ExistsReq, ExistsRes, GetReq, GetRes, PrintTestReq, PrintTestRes, SetReq,
    SetRes,
};
use bytes::Bytes;
use openraft::raft::{VoteRequest, VoteResponse};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use async_trait::async_trait; // 需要在 Cargo.toml 中添加 async-trait = "0.1"

pub type HandlerEntry = (u32, fn() -> Box<dyn RpcHandler>);

pub static HANDLER_TABLE: &[HandlerEntry] = &[
    (1, || Box::new(RpcMethod { func: print_test })),
    (2, || Box::new(RpcMethod { func: set })),
    (3, || Box::new(RpcMethod { func: get })),
    (4, || Box::new(RpcMethod { func: del })),
    (5, || Box::new(RpcMethod { func: exists })),
    (6, || Box::new(RpcMethod { func: vote })),
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

async fn set(_app: Arc<CacheCatApp>, req: SetReq) -> SetRes {
    let cache = get_cache();
    let v = MyValue {
        data: Arc::new(req.value),
        ttl_ms: req.ex_time,
    };
    cache.insert(req.key, v);
    SetRes {}
}

async fn get(_app: Arc<CacheCatApp>, req: GetReq) -> GetRes {
    let cache = get_cache();
    let a = cache.get(&req.key);
    GetRes {
        value: a.map(|v| v.data.clone()),
    }
}

async fn del(_app: Arc<CacheCatApp>, req: DelReq) -> DelRes {
    let cache = get_cache();
    match cache.remove(&req.key) {
        None => DelRes { num: 0 },
        Some(_) => DelRes { num: 1 },
    }
}

async fn exists(_app: Arc<CacheCatApp>, req: ExistsReq) -> ExistsRes {
    let cache = get_cache();
    if cache.contains_key(&req.key) {
        ExistsRes { num: 1 }
    } else {
        ExistsRes { num: 0 }
    }
}

// 核心修改点：vote 现在可以顺畅地使用 .await
async fn vote(app: Arc<CacheCatApp>, req: VoteRequest<TypeConfig>) -> VoteResponse<TypeConfig> {
    // openraft 的 vote 是异步的
    app.raft.vote(req).await.expect("Raft vote failed")
}