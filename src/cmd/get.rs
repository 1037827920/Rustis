use bytes::Bytes;
use tracing::{debug, instrument};

use crate::{
    networking::{connection::Connection, frame::Frame, parse::Parse},
    persistence::database::Database,
};

/// # Get 结构体
///
/// 获取key的值
///
/// # 语法
///
/// GET [key]
#[derive(Debug)]
pub struct Get {
    /// 键
    key: String,
}

impl Get {
    /// # new() 函数
    ///
    /// 创建一个新的Get命令
    pub(crate) fn new(key: impl ToString) -> Get {
        Get {
            key: key.to_string(),
        }
    }

    /// # key() 函数
    ///
    /// 获取键
    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    /// # parse_get_from_frame() 函数
    ///
    /// 从数据帧中解析出Get命令
    pub(crate) fn parse_get_from_frame(parse: &mut Parse) -> crate::Result<Get> {
        // GET命令已经被消费，所以下一个是键
        let key = parse.next_string()?;

        Ok(Get::new(key))
    }

    /// # apply() 函数
    ///
    /// 应用Get命令，并将响应写入到Connection实例
    #[instrument(skip(self))]
    pub(crate) async fn apply(
        self,
        db: &Database,
        connection: &mut Connection,
    ) -> crate::Result<()> {
        // 从database实例中获取value
        let response = if let Some(value) = db.get(&self.key) {
            Frame::Bulk(value)
        } else {
            Frame::Null
        };

        debug!(?response);

        // 将响应写入到connection实例
        connection.write_frame(&response).await?;

        Ok(())
    }

    /// # into_frame() 函数
    ///
    /// 在客户端中使用，用于将Ping命令编码到为一个数据帧发送到服务器
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("get".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));

        frame
    }
}
