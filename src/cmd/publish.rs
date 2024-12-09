//! publish命令实现

use bytes::Bytes;
use tracing::instrument;

use crate::{
    networking::{connection::Connection, frame::Frame, parse::Parse},
    persistence::database::Database,
};

/// # Publish 结构体
///
/// 将消息发布到给定的channel
#[derive(Debug)]
pub struct Publish {
    /// 发布消息的channel名
    channel: String,
    /// 发布的消息内容
    message: Bytes,
}

impl Publish {
    pub(crate) fn new(channel: impl ToString, message: Bytes) -> Self {
        Self {
            channel: channel.to_string(),
            message,
        }
    }

    /// # decode_publish_from_frame() 函数
    ///
    /// 将帧解码为publish命令
    pub(crate) fn decode_publish_from_frame(parse: &mut Parse) -> crate::Result<Self> {
        let channel = parse.next_string()?;
        let message = parse.next_bytes()?;

        Ok(Self::new(channel, message))
    }

    /// # code_publish_into_frame() 函数
    ///
    /// 将publish命令编码为帧
    pub(crate) fn code_publish_into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("publish".as_bytes()));
        frame.push_bulk(Bytes::from(self.channel.into_bytes()));
        frame.push_bulk(self.message);

        frame
    }

    /// # apply() 函数
    ///
    /// 应用publish命令，并将响应写入到Connection实例
    #[instrument(skip(self))]
    pub(crate) async fn apply(
        self,
        database: &Database,
        connection: &mut Connection,
    ) -> crate::Result<()> {
        let num_subscribers = database.publish(&self.channel, self.message);

        let response = Frame::Integer(num_subscribers as u64);
        connection.write_frame(&response).await?;

        Ok(())
    }
}
