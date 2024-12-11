//! Ping命令的实现

use bytes::Bytes;
use tracing::{debug, instrument};

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

    /// # decode_ping_from_frame() 函数
    ///
    /// 将帧解码为ping命令
    pub(crate) fn decode_ping_from_frame(parse: &mut Parse) -> crate::Result<Ping> {
        match parse.next_bytes() {
            Ok(msg) => Ok(Ping::new(Some(msg))),
            Err(ParseError::EndOfStream) => Ok(Ping::default()),
            Err(err) => Err(err.into()),
        }
    }

    /// # code_ping_into_frame() 函数
    ///
    /// 将ping命令编码为帧
    pub(crate) fn code_ping_into_frame(self) -> Frame {
        let mut frame: Frame = Frame::array();
        frame.push_bulk(Bytes::from("ping".as_bytes()));
        if let Some(msg) = self.msg {
            frame.push_bulk(msg);
        }
        frame
    }

    /// # apply() 函数
    ///
    /// 应用Ping命令，并将响应写入到Connection实例
    #[instrument(skip(self, connection))]
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
}
