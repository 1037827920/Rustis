//! Ping命令的实现

use bytes::Bytes;
use tracing::debug;

use crate::networking::{
    connection::Connection,
    frame::Frame,
    parse::{Parse, ParseError},
};

#[derive(Debug, Default)]
pub struct Ping {
    /// 消息（可选的
    msg: Option<Bytes>,
}

impl Ping {
    /// # new() 函数
    ///
    /// 创建一个新的Ping命令
    pub fn new(msg: Option<Bytes>) -> Self {
        Self { msg }
    }

    /// # parse_frames() 函数
    ///
    /// 从数据帧中解析出一个Ping实例
    ///
    /// # 语法
    ///
    /// PING [message]
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Ping> {
        match parse.next_bytes() {
            Ok(msg) => Ok(Ping::new(Some(msg))),
            Err(ParseError::EndOfStream) => Ok(Ping::default()),
            Err(err) => Err(err.into()),
        }
    }

    /// # apply() 函数
    ///
    /// 应用Ping命令，并将响应写入到Connection实例
    pub(crate) async fn apply(self, connection: &mut Connection) -> crate::Result<()> {
        let response = match self.msg {
            None => Frame::Simple("PONG".to_string()),
            Some(msg) => Frame::Bulk(msg),
        };

        debug!(?response);

        // 将响应写入到Connection实例
        connection.write_frame(&response).await?;

        Ok(())
    }

    /// # into_frame() 函数
    ///
    /// 在客户端中使用，用于将Ping命令编码到为一个数据帧发送到服务器
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("ping".as_bytes()));

        if let Some(msg) = self.msg {
            frame.push_bulk(msg);
        }

        frame
    }
}
