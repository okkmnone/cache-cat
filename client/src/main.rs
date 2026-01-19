use client::client::RpcClient;
use server::share::model::{SetReq, SetRes};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = RpcClient::connect("127.0.0.1:8080").await?;

    // let res2: SetRes = client
    //     .call(
    //         2,
    //         SetReq {
    //             key: "key".to_string(),
    //             value: Vec::from("value".to_string()),
    //             ex_time: 1000000,
    //         },
    //     )
    //     .await?;
    let res2: SetRes = client
        .call(
            2,
            SetReq {
                key: "key".to_string(),
                value: Vec::from("val11111ue".to_string()),
                ex_time: 10000,
            },
        )
        .await?;
    Ok(())
}
