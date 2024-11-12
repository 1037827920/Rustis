
use mini_redis::client;
use tokio::sync::{mpsc, oneshot};
use bytes::Bytes;


#[tokio::main]
async fn main() {
    // 创建一个容量为32的新channel（多生产者，单消费者
    let (tx0, mut rx) = mpsc::channel(32);

    // 通过clone发送方完成多个任务的发送
    let tx1 = tx0.clone();

    // 管理客户端资源的专用任务
    let manager = tokio::spawn(async move {
        // 建立到服务器的连接
        let mut client = client::connect("localhost:6379").await.unwrap();

        // 开始接收消息
        while let Some(cmd) = rx.recv().await {
            use Command::*;

            match cmd {
                Get { key , response} => {
                    let res = client.get(&key).await;
                    // 需要.await，因为oneshot总是会立即失败或成功，不需要任何形式的等待
                    let _  = response.send(res);
                }
                Set { key, value , response} => {
                    let res = client.set(&key, value).await;
                    let _ = response.send(res);
                }
            }
        }
    });

    let t1 = tokio::spawn(async move {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = Command::Get { 
            key: "foo".to_string(),
            response: response_tx,
        };

        // 发送GET请求
        tx0.send(cmd).await.unwrap();

        // 等待响应
        let response = response_rx.await;
        println!("GET foo: {:?}", response);
    });

    let t2 = tokio::spawn(async move {
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = Command::Set {
            key: "foo".to_string(),
            value: "bar".into(),
            response: response_tx,
        };

        // 发送SET请求
        tx1.send(cmd).await.unwrap();

        // 等待响应
        let response = response_rx.await;
        println!("SET foo: {:?}", response);
    });

    t1.await.unwrap();
    t2.await.unwrap();
    manager.await.unwrap();
}

// 为了避免跟mpsc::Sender冲突，将其别名为Responder
type Responder<T> = oneshot::Sender<mini_redis::Result<T>>;

#[derive(Debug)]
pub enum Command {
    Get { 
        key: String,
        response: Responder<Option<Bytes>>,
    },
    Set {
        key: String,
        value: Bytes,
        response: Responder<()>,
    },
}