//! Client结构体的实现，实现了Client的构造函数和一些方法

use bytes::Bytes;
use std::io::{Error, ErrorKind};
use tokio::net::{TcpStream, ToSocketAddrs};
use tracing::{debug, instrument};

use crate::{networking::{connection::Connection, frame::Frame}, cmd::ping::Ping};

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
