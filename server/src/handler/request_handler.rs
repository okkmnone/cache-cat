use bytes::{Buf, Bytes, BytesMut};
use fory::{Fory, ForyObject};
use std::net::SocketAddr;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

struct Decoder {
    request_id: u32,
    func_id: u32,
    data: Bytes,
}

pub async fn hand(
    mut socket: TcpStream,
    addr: SocketAddr,
    mut package: BytesMut,
) -> Result<(), ()> {
    //回显消息,同样添加4 byte的长度头
    let request_id = u32::from_be_bytes([package[0], package[1], package[2], package[3]]);
    let func_id = u32::from_be_bytes([package[4], package[5], package[6], package[7]]);
    package.advance(8);
    let decoder = Decoder {
        request_id,
        func_id,
        data: package.freeze(),
    };
    let response_data = if decoder.func_id == 1 {
        let mut fory = Fory::default();
        fory.register::<PrintTestReq>(1).unwrap();
        fory.register::<PrintTestRes>(2).unwrap();
        let req: PrintTestReq = fory.deserialize(decoder.data.as_ref()).unwrap();
        let res = print_test(req);
        fory.serialize(&res).unwrap()
    } else {
        return Err(());
    };
    let mut response_length = response_data.len() as u32;
    response_length = response_length + 4;
    if let Err(e) = socket.write_all(&response_length.to_be_bytes()).await {
        eprintln!("发送响应长度头失败 ({}): {}", addr, e);
        return Err(());
    }
    if let Err(e) = socket.write_all(&decoder.request_id.to_be_bytes()).await {
        eprintln!("发送响应长度头失败 ({}): {}", addr, e);
        return Err(());
    }
    if let Err(e) = socket.write_all(&*response_data).await {
        eprintln!("发送响应数据体失败 ({}): {}", addr, e);
        return Err(());
    }
    Ok(())
}

//每个方法和结构体 绑定一个数字
pub struct Request<T, U> {
    request_id: u32,
    req_data: Box<T>,
    res_data: Box<U>,
    func: fn(Box<T>) -> (Box<U>),
}

#[derive(ForyObject, Debug, PartialEq)]
pub struct PrintTestReq {
    pub message: String,
}

#[derive(ForyObject, Debug, PartialEq)]
pub struct PrintTestRes {
    pub message: String,
}

fn print_test(d: PrintTestReq) -> PrintTestRes {
    println!("{}", d.message);
    PrintTestRes { message: d.message }
}
