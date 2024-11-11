
use mini_redis::client;

#[tokio::main]
async fn main() {
    // 建立到服务器的连接
    let mut client = client::connect("localhost:6379").await.unwrap();

    // spawn两个任务，一个获取键，另一个设置键
    let task0 = tokio::spawn(async {
        let _res = client.get("foo").await;
    });
    let task1 = tokio::spawn(async {
        client.set("foo", "bar".into()).await;
    });

    // 等待两个任务完成
    task0.await.unwrap();
    task1.await.unwrap();
}