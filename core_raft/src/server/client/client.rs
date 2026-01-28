use bincode2;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use dashmap::DashMap; // 需要添加依赖
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::error::Error;
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

// 定义传输给写任务的消息结构
struct WriteRequest {
    data: Bytes,
}

/// 异步高并发 RPC 客户端
pub struct RpcClient {
    // 使用 mpsc 发送请求给后台写任务，完全移除 call 方法中的互斥锁
    tx_writer: mpsc::Sender<WriteRequest>,
    // 使用 DashMap 替换 Mutex<HashMap>，大幅减少并发下的锁竞争
    pending: Arc<DashMap<u32, oneshot::Sender<Bytes>>>,
    next_request_id: Arc<AtomicU32>,
}

impl RpcClient {
    pub async fn connect(addr: &str) -> Result<Self, Box<dyn Error>> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?; // 关键：禁用 Nagle 算法，减少小包延迟
        let (reader, writer) = stream.into_split();
        let pending = Arc::new(
            DashMap::<u32, oneshot::Sender<Bytes>>::new()
        );
        let pending_clone = pending.clone();

        // --- 写任务 (Write Loop) ---
        // 使用 buffer = 1024 的通道，起到背压作用
        let (tx_writer, mut rx_writer) = mpsc::channel::<WriteRequest>(1024);

        tokio::spawn(async move {
            // 使用 BufWriter 包装 TCP writer，合并小包写入，减少 Syscall
            let mut buf_writer = BufWriter::with_capacity(8 * 1024, writer);

            while let Some(req) = rx_writer.recv().await {
                if let Err(e) = buf_writer.write_all(&req.data).await {
                    eprintln!("RPC writer: 写入失败: {}", e);
                    break;
                }
                // 也可以根据策略定期 flush，或者由 BufWriter 自动处理
                // 在低延迟场景下，如果 mpsc 空了，立即 flush 是个好习惯
                if rx_writer.is_empty() {
                    if let Err(e) = buf_writer.flush().await {
                        eprintln!("RPC writer: flush 失败: {}", e);
                        break;
                    }
                }
            }
        });

        // --- 读任务 (Read Loop) ---
        tokio::spawn(async move {
            let mut reader = reader;
            // 复用 buffer 避免频繁分配，这里仅用于读取 header
            let mut len_buf = [0u8; 4];

            loop {
                // 1. 读取长度头
                if let Err(_) = reader.read_exact(&mut len_buf).await {
                    // 连接关闭或错误，不再打印详细堆栈，通常是服务器断开
                    break;
                }
                let resp_len = u32::from_be_bytes(len_buf) as usize;

                // 2. 读取 Body
                // 优化点：对于已知长度的数据，可以使用 BytesMut::with_capacity
                // 或者如果内存允许，复用一个足够大的 buffer。
                // 这里为了安全简单，针对每个包分配内存（在高性能场景下可以引入 Buffer Pool）
                let mut body = BytesMut::with_capacity(resp_len);
                unsafe {
                    // 预留空间，避免 resize (需要确保后续 read_exact 填满)
                    body.set_len(resp_len);
                }

                if let Err(e) = reader.read_exact(&mut body).await {
                    eprintln!("RPC reader: 读取 Body 失败: {}", e);
                    break;
                }

                // 3. 解析 Request ID
                if resp_len < 4 {
                    eprintln!("RPC reader: 协议错误，长度不足");
                    continue;
                }

                // 此时 body 包含: [request_id(4) | payload...]
                let request_id = u32::from_be_bytes(body[0..4].try_into().unwrap());

                // 去掉头部的 request_id，剩下的是真正的 payload
                let payload = body.split_off(4).freeze();

                // 4. 从 DashMap 取出 sender 并通知
                // DashMap 的 remove 是原子的，且分段锁竞争极小
                if let Some((_, tx)) = pending_clone.remove(&request_id) {
                    let _ = tx.send(payload);
                } else {
                    // 可能是请求超时了，sender 已经被丢弃
                }
            }

            // 清理工作：连接断开时，通知所有等待的请求
            pending_clone.clear(); // 简单清除，更严谨的做法是遍历并发送错误
        });

        Ok(Self {
            tx_writer,
            pending,
            next_request_id: Arc::new(AtomicU32::new(1)),
        })
    }

    pub async fn call<Req, Res>(
        &self,
        func_id: u32,
        req: Req,
    ) -> Result<Res, Box<dyn std::error::Error + Send + Sync>>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        // 1. 准备 ID
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);

        // 2. 序列化 (CPU 密集型操作在调用者线程完成，不阻塞 IO 线程)
        let payload = bincode2::serialize(&req)?;
        let frame_len = (4 + 4 + payload.len()) as u32; // req_id + func_id + payload

        // 3. 组装数据包 (Zero copy 比较难，BytesMut 已经很快了)
        let mut buf = BytesMut::with_capacity(4 + frame_len as usize);
        buf.put_u32(frame_len);
        buf.put_u32(request_id);
        buf.put_u32(func_id);
        buf.put_slice(&payload);

        let req_bytes = buf.freeze();

        // 4. 注册回调 (在发送前注册，防止 race condition)
        let (tx, rx) = oneshot::channel();
        self.pending.insert(request_id, tx);

        // 5. 发送给写任务 (这里不再有 Mutex 锁，只有 Channel 发送开销)
        // 如果写任务忙，这里会受到 channel 容量限制产生的背压
        if let Err(_) = self.tx_writer.send(WriteRequest { data: req_bytes }).await {
            self.pending.remove(&request_id);
            return Err("RPC connection closed".into());
        }

        // 6. 等待响应
        let start = Instant::now();
        let response_bytes = match rx.await {
            Ok(bytes) => bytes,
            Err(_) => {
                self.pending.remove(&request_id);
                return Err("RPC wait timeout or connection closed".into());
            }
        };
        tracing::info!("RPC 往返耗时: {} us", start.elapsed().as_micros());

        // 7. 反序列化
        let res: Res = bincode2::deserialize(&response_bytes)?;
        Ok(res)
    }
}