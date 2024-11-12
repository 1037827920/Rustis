use mini_redis::{client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // 创建一个客户端连接mini-redis
    let mut client = client::connect("localhost:6379").await?;

    // 设置一个键值对
    client.set("hello", "redis".into()).await?;

    // 获取一个键的值
    let result = client.get("hello").await?;

    println!("从数据库获取的值: {:?}", result);

    Ok(())
}
