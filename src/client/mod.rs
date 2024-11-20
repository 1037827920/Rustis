//! Client结构体的实现，实现了Client的构造函数和一些方法

use bytes::Bytes;
use std::io::{Error, ErrorKind};
use tokio::net::{TcpStream, ToSocketAddrs};
use tracing::{debug, instrument};

use crate::{
    cmd::ping::Ping,
    networking::{connection::Connection, frame::Frame},
};

/// # Client 结构体
///
/// Client结构体是一个客户端结构体，用于与服务器进行通信
pub struct Client {
    /// Connection实例
    connection: Connection,
}

impl Client {
    /// # connection() 函数
    ///
    /// 根据ip地址与远程服务器建立连接
    ///
    /// # 参数
    ///
    /// - `addr`: 远程服务器的ip地址, 可以是任何可以异步转换为SocketAddr的类型，ToSocketAddrs trait是tokio版本不是std版本
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<Client> {
        // 通过TcpStream::connect()函数与远程服务器建立连接, 返回一个socket
        let socket = TcpStream::connect(addr).await?;

        // 初始化Connection实例，将socket传入，会为其分配读写缓冲区来执行redis协议帧解析
        let connection = Connection::new(socket);

        Ok(Client { connection })
    }

    /// # ping() 函数
    ///
    /// 向服务器发送ping命令，如果没有提供参数就返回PONG，该命令常用于测试连接是否存活
    #[instrument(skip(self))]
    pub async fn ping(&mut self, msg: Option<Bytes>) -> crate::Result<Bytes> {
        // 将Ping命令编码为数据帧，方便传输到服务器
        let frame = Ping::new(msg).into_frame();
        debug!(request = ?frame);
        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Simple(value) => Ok(value.into()),
            Frame::Bulk(value) => Ok(value),
            frame => Err(frame.to_error()),
        }
    }

    /// # read_response() 函数
    ///
    /// 从socket中读取响应帧
    async fn read_response(&mut self) -> crate::Result<Frame> {
        // 读取响应帧
        let response = self.connection.read_frame().await?;
        debug!(?response);

        match response {
            Some(Frame::Error(msg)) => Err(msg.into()),
            Some(frame) => Ok(frame),
            None => {
                // 响应为None表示服务器已经关闭这个客户端的连接
                let error = Error::new(ErrorKind::ConnectionReset, "连接被服务器重置");
                Err(error.into())
            }
        }
    }
}

mod tests {

    use super::*;
    use crate::networking::frame;
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn test_ping() -> crate::Result<()> {
        // 创建一个TcpListener
        let listener = TcpListener::bind("localhost:0").await?;
        // local_addr用于获取socket绑定的本地地址
        let addr = listener.local_addr()?;

        // 创建一个任务来模拟服务器
        let server = tokio::spawn(async move {
            let (mut server_stream, _) = listener.accept().await.unwrap();
            let mut connection = Connection::new(server_stream);

            // 读取客户端发送的帧
            if let Some(frame) = connection.read_frame().await.unwrap() {
                match frame {
                    Frame::Array(_) => {
                        // 服务器响应PONG
                        connection
                            .write_frame(&Frame::Simple("PONG".to_string()))
                            .await.unwrap();
                    }
                    _ => panic!("无效的帧类型"),
                }
            }
        });

        // 创建一个客户端来连接到服务器
        let mut client = Client::connect(addr).await?;

        // 发送ping命令
        let reponse = client.ping(None).await?;

        // 检查响应是否为PONG
        assert_eq!(reponse, String::from("PONG"));

        // 等待服务器任务结束
        server.await?;

        Ok(())
    }
}
