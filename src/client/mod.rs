//! Client结构体的实现，实现了Client的构造函数和一些方法

use async_stream::try_stream;
use bytes::Bytes;
use std::{
    io::{Error, ErrorKind},
    time::Duration,
};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_stream::Stream;
use tracing::{debug, instrument};

use crate::{
    cmd::{
        get::Get,
        ping::Ping,
        publish::Publish,
        set::Set,
        subscribe::{Subscribe, Unsubscribe},
    },
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

    /// # ping() 函数
    ///
    /// 向服务器编码并发送ping命令，如果没有提供参数就返回PONG，该命令常用于测试连接是否存活
    #[instrument(skip(self))]
    pub async fn ping(&mut self, msg: Option<Bytes>) -> crate::Result<Bytes> {
        // 将Ping命令编码为数据帧，方便传输到服务器
        let frame = Ping::new(msg).code_ping_into_frame();
        debug!(request = ?frame);
        // 将帧写入connection中
        self.connection.write_frame(&frame).await?;

        // 读取服务器的响应
        match self.read_response().await? {
            Frame::Simple(value) => Ok(value.into()),
            Frame::Bulk(value) => Ok(value),
            frame => Err(frame.to_error()),
        }
    }

    /// # get() 函数
    ///
    /// 向服务器编码并发送get命令，获取key的值
    #[instrument(skip(self))]
    pub async fn get(&mut self, key: &str) -> crate::Result<Option<Bytes>> {
        // 将Get命令编码为数据库帧，方便传输到服务器
        let frame = Get::new(key).code_get_into_frame();
        debug!(request = ?frame);
        // 将帧写入到连接中
        self.connection.write_frame(&frame).await?;

        // 读取服务器的响应
        match self.read_response().await? {
            Frame::Simple(value) => Ok(Some(value.into())),
            Frame::Bulk(value) => Ok(Some(value)),
            Frame::Null => Ok(None),
            frame => Err(frame.to_error()),
        }
    }

    /// # set() 函数
    ///
    /// 向服务器编码并发送set命令，设置key的值
    #[instrument(skip(self))]
    pub async fn set(&mut self, key: &str, value: Bytes) -> crate::Result<()> {
        self.set_cmd(Set::new(key, value, None)).await
    }

    /// # set_with_expires() 函数
    ///
    /// 向服务器编码并发送set命令，设置key的值，并设置过期时间
    #[instrument(skip(self))]
    pub async fn set_with_expires(
        &mut self,
        key: &str,
        value: Bytes,
        expires: Duration,
    ) -> crate::Result<()> {
        self.set_cmd(Set::new(key, value, Some(expires))).await
    }

    /// # set_cmd() 函数
    ///
    /// set命令的核心实现
    #[instrument(skip(self))]
    pub async fn set_cmd(&mut self, cmd: Set) -> crate::Result<()> {
        // 将set命令编码为数据库帧，方便传输到服务器
        let frame = cmd.code_set_into_frame();
        debug!(request = ?frame);

        // 将帧写入到流中
        self.connection.write_frame(&frame).await?;

        // 读取服务器的响应
        match self.read_response().await? {
            Frame::Simple(response) if response.to_uppercase() == "OK" => Ok(()),
            frame => Err(frame.to_error()),
        }
    }

    /// # publish() 函数
    ///
    /// 向服务器编码并发送publish命令，将消息发布到给定的channel
    #[instrument(skip(self))]
    pub async fn publish(&mut self, channel: &str, message: Bytes) -> crate::Result<u64> {
        // 将publish命令编码为数据库帧，方便传输到服务器
        let frame = Publish::new(channel, message).code_publish_into_frame();
        debug!(request = ?frame);

        // 将帧写入到流中
        self.connection.write_frame(&frame).await?;

        // 读取服务器的响应
        match self.read_response().await? {
            Frame::Integer(response) => Ok(response as u64),
            frame => Err(frame.to_error()),
        }
    }

    /// # subscribe() 函数
    ///
    /// 将客户端订阅到指定channel，返回一个Subscriber实例
    #[instrument(skip(self))]
    pub async fn subscribe(mut self, channels: Vec<String>) -> crate::Result<Subscriber> {
        self.subscribe_cmd(&channels).await?;

        Ok(Subscriber::new(self, channels))
    }

    /// # subscribe_cmd() 函数
    ///
    /// subscribe命令的核心实现
    async fn subscribe_cmd(&mut self, channels: &[String]) -> crate::Result<()> {
        // 将subscribe命令编码为帧
        let frame = Subscribe::new(channels.to_vec()).code_subscribe_into_frame();
        debug!(request = ?frame);

        // 将帧写入到连接中
        self.connection.write_frame(&frame).await?;

        // 读取服务器的响应
        for channel in channels {
            let response = self.read_response().await?;

            match response {
                Frame::Array(ref frame) => match frame.as_slice() {
                    [subscribe, cha, ..] if *subscribe == "subscribe" && *cha == channel => {}
                    _ => return Err(response.to_error()),
                },
                frame => return Err(frame.to_error()),
            };
        }

        Ok(())
    }
}

/// # Subscriber 结构体
///
/// 一旦客户端订阅了一个channel，它们只能执行pub/sub相关的命令。
/// Client类型被转换为Subscriber类型，防止调用非pub/sub相关的命令。
pub struct Subscriber {
    /// Client实例
    client: Client,
    /// 当前订阅的channels
    subscriber_channels: Vec<String>,
}

impl Subscriber {
    pub fn new(client: Client, subscriber_channels: Vec<String>) -> Self {
        Self {
            client,
            subscriber_channels,
        }
    }

    /// # get_subscriber() 函数
    ///
    /// 获取当前订阅的channels
    pub fn get_subscriber_channels(&self) -> &[String] {
        &self.subscriber_channels
    }

    /// # next_message() 函数
    ///
    /// 从订阅的channels中接收消息。
    /// None表示channels已经被关闭
    pub async fn next_message(&mut self) -> crate::Result<Option<Message>> {
        match self.client.connection.read_frame().await? {
            Some(frame) => {
                debug!(?frame);

                match frame {
                    Frame::Array(ref f) => match f.as_slice() {
                        [message, channel, content] if *message == "message" => Ok(Some(
                            Message::new(channel.to_string(), Bytes::from(content.to_string())),
                        )),
                        _ => Err(frame.to_error()),
                    },
                    f => Err(f.to_error()),
                }
            }
            None => Ok(None),
        }
    }

    /// # into_stream() 函数
    ///
    /// 通过async-stream crate将Subscriber转换为Stream。
    /// 将Subcriber转换为Stream，是为了简化异步消息处理
    pub(crate) fn into_stream(mut self) -> impl Stream<Item = crate::Result<Message>> {
        try_stream! {
            while let Some(message) = self.next_message().await? {
                yield message;
            }
        }
    }

    /// # into_client() 函数
    /// 
    /// 从Subscriber对象中取回Client对象所有权
    pub fn into_client(self) -> Client {
        self.client
    }

    /// # subscribe() 函数
    ///
    /// 订阅channels
    #[instrument(skip(self))]
    pub(crate) async fn subscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        // 向服务器发出subscribe命令并等待确认
        self.client.subscribe_cmd(channels).await?;

        // 更新订阅的channels
        self.subscriber_channels
            .extend(channels.iter().map(Clone::clone));

        Ok(())
    }

    /// # unsubscribe() 函数
    ///
    /// 取消订阅channels
    #[instrument(skip(self))]
    pub(crate) async fn unsubscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        // 将unsubscribe命令编码为帧
        let frame = Unsubscribe::new(channels.to_vec()).code_unsubscribe_into_frame();
        debug!(request = ?frame);

        // 将帧写入到连接中
        self.client.connection.write_frame(&frame).await?;

        // 如果channels为空，则取消订阅所有channels
        let num = if channels.is_empty() {
            self.subscriber_channels.len()
        } else {
            channels.len()
        };

        // 读取服务器的响应
        for _ in 0..num {
            let response = self.client.read_response().await?;

            match response {
                Frame::Array(ref frame) => match frame.as_slice() {
                    [unsubscribe, channel, ..] if *unsubscribe == "unsubscribe" => {
                        let len = self.subscriber_channels.len();

                        if len == 0 {
                            return Err(response.to_error());
                        }

                        // 想要取消的channel应该在订阅的channels中
                        self.subscriber_channels.retain(|c| *channel != &c[..]);

                        // 每次只取消一个subcription
                        if self.subscriber_channels.len() != len - 1 {
                            return Err(response.to_error());
                        }
                    }
                    _ => return Err(response.to_error()),
                },
                frame => return Err(frame.to_error()),
            };
        }

        Ok(())
    }
}

/// # Message 结构体
///
/// 从chennel中接收到的消息
#[derive(Debug)]
pub struct Message {
    pub channel: String,
    pub message: Bytes,
}

impl Message {
    pub fn new(channel: String, message: Bytes) -> Self {
        Self { channel, message }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_ping() -> crate::Result<()> {
        // 创建一个TcpListener
        let listener = TcpListener::bind("localhost:0").await?;
        // local_addr用于获取socket绑定的本地地址
        let addr = listener.local_addr()?;

        // 创建一个任务来模拟服务器
        let server = tokio::spawn(async move {
            let (server_stream, _) = listener.accept().await.unwrap();
            let mut connection = Connection::new(server_stream);

            // 读取客户端发送的帧
            if let Some(frame) = connection.read_frame().await.unwrap() {
                match frame {
                    Frame::Array(_) => {
                        // 服务器响应PONG
                        connection
                            .write_frame(&Frame::Simple("PONG".to_string()))
                            .await
                            .unwrap();
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

    #[tokio::test]
    async fn test_get() -> crate::Result<()> {
        let listener = TcpListener::bind("localhost:0").await?;
        let addr = listener.local_addr()?;

        // 创建一个任务来模拟服务器
        let server = tokio::spawn(async move {
            let (server_stream, _) = listener.accept().await.unwrap();
            let mut connection = Connection::new(server_stream);

            // 读取客户端发送的帧
            if let Some(frame) = connection.read_frame().await.unwrap() {
                match frame {
                    Frame::Array(parts) => {
                        // 检查是否是get命令
                        if let Some(Frame::Bulk(cmd)) = parts.get(0) {
                            // 将Bytes转换为&str
                            let cmd = std::str::from_utf8(&cmd).unwrap();
                            if cmd.to_lowercase() == "get" {
                                // 服务器响应值
                                connection
                                    .write_frame(&Frame::Bulk(Bytes::from("test_value".as_bytes())))
                                    .await
                                    .unwrap();
                            } else {
                                panic!("Unexpected command");
                            }
                        }
                    }
                    _ => panic!("Unexpected frame type"),
                }
            }
        });

        // 创建一个客户端来连接到服务器
        let mut client = Client::connect(addr).await?;

        // 发送get命令
        let key = "test_key";
        let result = client.get(key).await?;

        // 检查响应是否为test_value
        assert_eq!(result, Some(Bytes::from("test_value")));

        // 等待服务器任务结束
        server.await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_set() -> crate::Result<()> {
        let listener = TcpListener::bind("localhost:0").await?;
        let addr = listener.local_addr()?;

        // 创建一个任务来模拟服务器
        let server = tokio::spawn(async move {
            let (server_stream, _) = listener.accept().await.unwrap();
            let mut connection = Connection::new(server_stream);

            // 读取客户端发送的帧
            if let Some(frame) = connection.read_frame().await.unwrap() {
                match frame {
                    Frame::Array(parts) => {
                        // 检查是否为set命令
                        if let Some(Frame::Bulk(cmd)) = parts.get(0) {
                            let cmd = std::str::from_utf8(&cmd).unwrap();
                            if cmd.to_lowercase() == "set" {
                                // 服务器响应OK
                                connection
                                    .write_frame(&Frame::Simple("OK".to_string()))
                                    .await
                                    .unwrap();
                            } else {
                                panic!("Unexpected command");
                            }
                        }
                    }
                    _ => panic!("Unexpected frame type"),
                }
            }
        });

        // 创建一个客户端来连接到服务器
        let mut client = Client::connect(addr).await?;

        // 发送set命令
        let key = "test_key";
        let value = Bytes::from("test_value");
        client.set(key, value).await?;

        // 等待服务器任务结束
        server.await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_publish() -> crate::Result<()> {
        let listener = TcpListener::bind("localhost:0").await?;
        let addr = listener.local_addr()?;

        // 创建一个任务来模拟服务器
        let server = tokio::spawn(async move {
            let (server_stream, _) = listener.accept().await.unwrap();
            let mut connection = Connection::new(server_stream);

            // 读取客户端发送的帧
            if let Some(frame) = connection.read_frame().await.unwrap() {
                match frame {
                    Frame::Array(parts) => {
                        if let Some(Frame::Bulk(cmd)) = parts.get(0) {
                            let cmd = std::str::from_utf8(&cmd).unwrap();
                            if cmd.to_lowercase() == "publish" {
                                // 服务器响应1
                                connection.write_frame(&Frame::Integer(1)).await.unwrap();
                            } else {
                                panic!("Unexpected command");
                            }
                        }
                    }
                    _ => panic!("Unexpected frame type"),
                }
            }
        });

        // 创建一个客户端来连接到服务器
        let mut client = Client::connect(addr).await?;

        // 发送publish命令
        let channel = "test_channel";
        let message = Bytes::from("test_message");
        let result = client.publish(channel, message).await?;

        // 检查响应是否为1
        assert_eq!(result, 1);

        // 等待服务器任务结束
        server.await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_subscribe() -> crate::Result<()> {
        let listener = TcpListener::bind("localhost:0").await?;
        let addr = listener.local_addr()?;

        // 创建一个任务来模拟服务器
        let server = tokio::spawn(async move {
            let (server_stream, _) = listener.accept().await.unwrap();
            let mut connection = Connection::new(server_stream);

            // 读取客户端发送的帧
            if let Some(frame) = connection.read_frame().await.unwrap() {
                match frame {
                    Frame::Array(parts) => {
                        if let Some(Frame::Bulk(cmd)) = parts.get(0).cloned() {
                            let cmd_str = std::str::from_utf8(&cmd).unwrap();
                            if cmd_str.eq_ignore_ascii_case("subscribe") {
                                // 响应订阅确认消息
                                for channel_frame in parts.iter().skip(1) {
                                    if let Frame::Bulk(channel) = channel_frame.clone() {
                                        let channel_str =
                                            String::from_utf8(channel.to_vec()).unwrap();
                                        // 模拟服务器发送订阅成功的响应
                                        let response = Frame::Array(vec![
                                            Frame::Bulk(Bytes::from("subscribe")),
                                            Frame::Bulk(Bytes::from(channel_str)),
                                            Frame::Integer(1),
                                        ]);
                                        connection.write_frame(&response).await.unwrap();
                                    }
                                }
                            } else {
                                panic!("Unexpected command");
                            }
                        }
                    }
                    _ => panic!("Unexpected frame type"),
                }
            }
        });

        // 创建一个客户端并连接到服务器
        let client = Client::connect(addr).await?;

        // 订阅频道
        let channels = vec!["test_channel".to_string()];
        let subscriber = client.subscribe(channels.clone()).await?;

        // 检查订阅的频道是否正确
        assert_eq!(subscriber.get_subscriber_channels(), channels.as_slice());

        // 等待服务器任务完成
        server.await?;

        Ok(())
    }
}
