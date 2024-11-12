use bytes::Bytes;
use mini_redis::{Connection, Frame};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::net::{TcpListener, TcpStream};

type Db = Arc<Mutex<HashMap<String, Bytes>>>;

#[tokio::main]
async fn main() {
    // 将服务器绑定到6730端口并监听连接
    let listener = TcpListener::bind("localhost:6379").await.unwrap();

    println!("开始监听");

    let db = Arc::new(Mutex::new(HashMap::new()));

    // 循环接收所有传入的连接
    loop {
        // _ : 包含新连接的IP和端口
        let (socket, _) = listener.accept().await.unwrap();
        let db = db.clone();

        println!("接收到连接");
        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
}

/// 处理传入的连接
async fn process(socket: TcpStream, db: Db) {
    use mini_redis::Command::{self, Get, Set};

    // Connection结构体可以让我们读写redis帧而不是字节流
    let mut connection = Connection::new(socket);

    // 使用read_frame接收来自连接的命令
    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let mut db_guard = db.lock().unwrap();
                db_guard.insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                let db_guard = db.lock().unwrap();
                if let Some(value) = db_guard.get(cmd.key()) {
                    Frame::Bulk(value.clone().into())
                } else {
                    Frame::Null
                }
            }
            cmd => panic!("unimplemented {:?}", cmd),
        };

        // 将响应写回客户端
        connection.write_frame(&response).await.unwrap();
    }
}
