use crate::core::moka::{MyValue, get_cache};
use crate::share::model::{
    DelReq, DelRes, ExistsReq, ExistsRes, GetReq, GetRes, PrintTestReq, PrintTestRes, SetReq,
    SetRes,
};
use bytes::Bytes;
use fory_core::Fory;
use std::sync::Arc;

pub type HandlerEntry = (u32, fn() -> Box<dyn RpcHandler>);
pub static HANDLER_TABLE: &[HandlerEntry] = &[
    (1, || Box::new(RpcMethod { func: print_test })),
    (2, || Box::new(RpcMethod { func: set })),
    (3, || Box::new(RpcMethod { func: get })),
    (4, || Box::new(RpcMethod { func: del })),
    (5, || Box::new(RpcMethod { func: exists })),
];
pub trait RpcHandler: Send + Sync {
    fn call(&self, fory: &Fory, data: Bytes) -> Bytes;
}
pub struct RpcMethod<Req, Res> {
    func: fn(Req) -> Res,
}

impl<Req, Res> RpcHandler for RpcMethod<Req, Res>
where
    Req: Send + 'static + fory::ForyDefault + fory::Serializer,
    Res: Send + 'static + fory::Serializer,
{
    fn call(&self, fory: &Fory, data: Bytes) -> Bytes {
        let req: Req = fory.deserialize(data.as_ref()).unwrap();
        let res = (self.func)(req);
        fory.serialize(&res).unwrap().into()
    }
}

fn print_test(d: PrintTestReq) -> PrintTestRes {
    println!("{}", d.message);
    PrintTestRes { message: d.message }
}

fn set(req: SetReq) -> SetRes {
    let cache = get_cache();
    let v = MyValue {
        data: Arc::new(req.value),
        ttl_ms: req.ex_time,
    };
    cache.insert(req.key, v);
    SetRes {}
}

fn get(req: GetReq) -> GetRes {
    let cache = get_cache();
    let a = cache.get(&req.key);
    //为避免空指针，返回Option
    GetRes {
        value: a.map(|v| v.data.clone()),
    }
}

fn del(req: DelReq) -> DelRes {
    let cache = get_cache();
    match cache.remove(&req.key) {
        None => DelRes { num: 0 },
        Some(_) => DelRes { num: 1 },
    }
}
fn exists(req: ExistsReq) -> ExistsRes {
    let cache = get_cache();
    if cache.contains_key(&req.key) {
        ExistsRes { num: 1 }
    } else {
        ExistsRes { num: 0 }
    }
}
