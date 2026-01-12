use bytes::{Buf, BufMut, BytesMut};
use fory_core::Fory;
use fory_derive::ForyObject;
use server::handler::request_handler::{PrintTestReq, PrintTestRes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:8080";
    let mut stream = TcpStream::connect(addr).await?;

    // ---------- Fory 初始化 ----------
    let mut fory = Fory::default();
    fory.register::<PrintTestReq>(1)?;
    fory.register::<PrintTestRes>(2)?;

    // ---------- 构造请求 ----------
    let request_id: u32 = 1;
    let func_id: u32 = 1;

    let req = PrintTestReq {
        message: "Hello from client".to_string(),
    };

    let payload = fory.serialize(&req)?;
    let length = (4 + 4 + payload.len()) as u32;

    let mut buf = BytesMut::with_capacity(4 + length as usize);
    buf.put_u32(length);
    buf.put_u32(request_id);
    buf.put_u32(func_id);
    buf.extend_from_slice(&payload);

    // ---------- 发送 ----------
    stream.write_all(&buf).await?;

    // ---------- 接收响应 ----------
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let resp_len = u32::from_be_bytes(len_buf) as usize;

    let mut resp_buf = BytesMut::with_capacity(resp_len);
    resp_buf.resize(resp_len, 0);
    stream.read_exact(&mut resp_buf).await?;

    let mut resp_buf = resp_buf.freeze();

    let resp_request_id = resp_buf.get_u32();
    assert_eq!(resp_request_id, request_id);

    let res: PrintTestRes = fory.deserialize(resp_buf.as_ref())?;
    println!("Response: {:?}", res);

    Ok(())
}
