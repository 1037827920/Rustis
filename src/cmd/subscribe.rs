//! subscribe命令实现

use std::{pin::Pin, vec};

use bytes::Bytes;
use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamExt, StreamMap};

use crate::{
    networking::{
        connection::Connection,
        frame::Frame,
        parse::{
            Parse,
            ParseError::{self, EndOfStream},
        },
    },
    persistence::database::Database,
    server::shutdown::Shutdown,
};

use super::{Command, Unknown};

/// # Subscribe 结构体
///
/// 将客户端订阅到一个或多个channel
#[derive(Debug)]
pub struct Subscribe {
    /// 频道
    channels: Vec<String>,
}

impl Subscribe {
    pub(crate) fn new(channels: Vec<String>) -> Self {
        Self { channels }
    }

    /// # decode_subscribe_from_frame() 函数
    ///
    /// 将帧解码为subscribe命令
    pub(crate) fn decode_subscribe_from_frame(parse: &mut Parse) -> crate::Result<Subscribe> {
        use ParseError::EndOfStream;

        let mut channels = vec![parse.next_string()?];

        loop {
            match parse.next_string() {
                Ok(channel) => channels.push(channel),
                Err(EndOfStream) => break,
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Self::new(channels))
    }

    /// # code_subscribe_into_frame() 函数
    ///
    /// 将subscribe命令编码为帧
    pub(crate) fn code_subscribe_into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("subscribe".as_bytes()));
        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }
        frame
    }

    /// # apply() 函数
    ///
    /// 应用Subscribe命令，并将响应写入到Connection实例
    pub(crate) async fn apply(
        mut self,
        database: &Database,
        connection: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        // 一个客户端可以订阅多个channel，并且可以动态地添加和移除其订阅的频道
        // 为了处理多个channel的订阅，使用StreamMap来跟踪活跃的订阅
        let mut subscriptions = StreamMap::new();

        loop {
            // 将需要订阅的channel添加到StreamMap中
            for channel_name in &self.channels {
                subscribe_to_channel(
                    channel_name.clone(),
                    &mut subscriptions,
                    database,
                    connection,
                )
                .await?;
            }

            // 等待以下事件发生：
            // - 从一个订阅channel接收消息
            // - 从客户端接收订阅或取消订阅的请求
            // - 服务器关闭信号
            tokio::select! {
                // 从订阅channel接收消息
                Some((channel_name, msg)) = subscriptions.next() => {
                    connection.write_frame(&create_message_frame(channel_name, msg)).await?;
                }
                // 从客户端接收订阅或取消订阅的请求
                res = connection.read_frame() => {
                    let frame = match res? {
                        Some(frame) => frame,
                        // 如果远程客户端已经关闭连接，则走到这里
                        None => return Ok(())
                    };

                    handle_command(frame, &mut self.channels, &mut subscriptions, connection).await?;
                }
                _ = shutdown.receiving() => {
                    return Ok(());
                }
            };
        }
    }
}

type Messages = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

/// # create_subscribe_response_frame() 函数
///
/// 创建一个subscribe响应帧
fn create_subscribe_response_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"subscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

/// # create_unsubscribe_response_frame() 函数
///
/// 创建一个unsubscribe响应帧
fn create_unsubscribe_response_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"unsubscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

/// # create_message_frame() 函数
///
/// 创建一个消息帧
fn create_message_frame(channel_name: String, message: Bytes) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"message"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_bulk(message);
    response
}

/// # subscribe_to_channel() 函数
///
/// 订阅一个channel
async fn subscribe_to_channel(
    channel_name: String,
    subscriptions: &mut StreamMap<String, Messages>,
    database: &Database,
    connection: &mut Connection,
) -> crate::Result<()> {
    let mut rx = database.subscribe(channel_name.clone());

    // 订阅一个channel
    let rx = Box::pin(async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(msg) => yield msg,
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => break,
            }
        }
    });

    // 跟踪客户端订阅集合中的订阅
    subscriptions.insert(channel_name.clone(), rx);

    // 响应客户端
    let response = create_subscribe_response_frame(channel_name, subscriptions.len());
    connection.write_frame(&response).await?;

    Ok(())
}

/// # handle_command() 函数
///
/// 处理在Subcriber::apply中的命令
async fn handle_command(
    frame: Frame,
    subscribe_to: &mut Vec<String>,
    subcriptions: &mut StreamMap<String, Messages>,
    connection: &mut Connection,
) -> crate::Result<()> {
    match Command::decode_cmd_from_frame(frame)? {
        Command::Subscribe(subscribe) => {
            subscribe_to.extend(subscribe.channels.into_iter());
        }
        Command::Unsubscribe(mut unsubscribe) => {
            // 如果unsubscribe为空，则要取消所有的channel订阅
            if unsubscribe.channels.is_empty() {
                unsubscribe.channels = subcriptions
                    .keys()
                    .map(|channel_name| channel_name.to_string())
                    .collect();
            }

            for channel_name in unsubscribe.channels {
                subcriptions.remove(&channel_name);

                let response = create_unsubscribe_response_frame(channel_name, subcriptions.len());
                connection.write_frame(&response).await?;
            }
        }
        command => {
            let cmd = Unknown::new(command.get_name());
            cmd.apply(connection).await?;
        }
    }
    Ok(())
}

/// # Unsubscribe 结构体
///
/// 将客户端从一个或多个channel取消订阅
#[derive(Debug)]
pub struct Unsubscribe {
    channels: Vec<String>,
}

impl Unsubscribe {
    pub(crate) fn new(channels: Vec<String>) -> Self {
        Self { channels }
    }

    /// # decode_unsubscribe_from_frame() 函数
    ///
    /// 将帧解码为unsubscribe命令
    pub(crate) fn decode_unsubscribe_from_frame(
        parse: &mut Parse,
    ) -> Result<Unsubscribe, ParseError> {
        let mut channels = vec![];

        loop {
            match parse.next_string() {
                Ok(s) => channels.push(s),
                Err(EndOfStream) => break,
                Err(err) => return Err(err),
            }
        }

        Ok(Unsubscribe::new(channels))
    }

    /// # code_unsubscribe_into_frame() 函数
    ///
    /// 将unsubscribe命令编码为帧
    #[allow(dead_code)]
    pub(crate) fn code_unsubscribe_into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("unsubscribe".as_bytes()));
        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }
        frame
    }
}
