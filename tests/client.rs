use rustis::{client::Client, server};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// # start_server() 函数
///
/// 启动一个服务器实例，返回服务器的地址和一个JoinHandle
async fn start_server() -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move { server::run(listener, tokio::signal::ctrl_c()).await });

    (addr, handle)
}

/// 测试一个简单的PING PONG
#[tokio::test]
async fn ping_pong_without_message() {
    let (addr, _) = start_server().await;
    let mut client = Client::connect(addr).await.unwrap();

    let pong = client.ping(None).await.unwrap();
    assert_eq!(b"PONG", &pong[..]);
}

/// 测试一个带有消息的PING PONG
#[tokio::test]
async fn ping_pong_with_message() {
    let (addr, _) = start_server().await;
    let mut client = Client::connect(addr).await.unwrap();

    let pong = client.ping(Some("Hello Rustis".into())).await.unwrap();
    assert_eq!("Hello Rustis".as_bytes(), &pong[..]);
}

/// 测试一个简单的GET SET
#[tokio::test]
async fn key_value_get_set() {
    let (addr, _) = start_server().await;

    let mut client = Client::connect(addr).await.unwrap();
    client.set("hello", "rustis".into()).await.unwrap();

    let value = client.get("hello").await.unwrap().unwrap();
    assert_eq!(b"rustis", &value[..])
}

/// 测试有过期的GET SET
#[tokio::test]
async fn key_value_timeout() {
    // 暂停tokio内部时间的流逝
    tokio::time::pause();

    let (addr, _) = start_server().await;

    let mut client = Client::connect(addr).await.unwrap();
    client
        .set_with_expires("hello", "rustis".into(), Duration::from_secs(1))
        .await
        .unwrap();

    // 等待1秒
    tokio::time::sleep(Duration::from_secs(1)).await;

    let value = client.get("hello").await.unwrap();
    assert!(value.is_none())
}

/// 测试一个简单的PUBLISH SUBSCRIBE
#[tokio::test]
async fn receive_message_subscribed_channel() {
    let (addr, _) = start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let mut subscriber = client.subscribe(vec!["hello".into()]).await.unwrap();

    tokio::spawn(async move {
        let mut client = Client::connect(addr).await.unwrap();
        client.publish("hello", "rustis".into()).await.unwrap()
    });

    let message = subscriber.next_message().await.unwrap().unwrap();
    assert_eq!("hello", &message.channel);
    assert_eq!(b"rustis", &message.content[..])
}

/// 测试客户端是否能从多个订阅的channel中接收消息
#[tokio::test]
async fn receive_message_multiple_subscribed_channels() {
    let (addr, _) = start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let mut subscriber = client
        .subscribe(vec!["hello".into(), "rustis".into()])
        .await
        .unwrap();

    tokio::spawn(async move {
        let mut client = Client::connect(addr).await.unwrap();
        client.publish("hello", "rustis".into()).await.unwrap()
    });

    let message1 = subscriber.next_message().await.unwrap().unwrap();
    assert_eq!("hello", &message1.channel);
    assert_eq!(b"rustis", &message1.content[..]);

    tokio::spawn(async move {
        let mut client = Client::connect(addr).await.unwrap();
        client.publish("rustis", "hello".into()).await.unwrap()
    });

    let message2 = subscriber.next_message().await.unwrap().unwrap();
    assert_eq!("rustis", &message2.channel);
    assert_eq!(b"hello", &message2.content[..])
}

/// 测试客户端是否能退订多个channel
#[tokio::test]
async fn unsubscribes_from_channels() {
    let (addr, _) = start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let mut subscriber = client
        .subscribe(vec!["hello".into(), "world".into()])
        .await
        .unwrap();

    subscriber.unsubscribe(&[]).await.unwrap();
    assert_eq!(subscriber.get_subscriber_channels().len(), 0);
}
