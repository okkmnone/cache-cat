

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    server::handler::rpc::main().await
}
