//! del命令的实现

use bytes::Bytes;
use tracing::debug;

use crate::networking::{connection::Connection, frame::Frame};

use super::{Database, Parse};

#[derive(Debug)]
pub struct Del {
    /// 键
    key: String,
}

impl Del {
    pub(crate) fn new(key: impl ToString) -> Del {
        Del {
            key: key.to_string(),
        }
    }

    /// # decode_del_from_frame() 函数
    ///
    /// 将帧解码为del命令
    pub(crate) fn decode_del_from_frame(parse: &mut Parse) -> crate::Result<Del> {
        // DEL命令已经被消费，所以下一个是键
        let key = parse.next_string()?;

        Ok(Del::new(key))
    }

    /// # code_del_into_frame() 函数
    ///
    /// 将del命令编码为帧
    pub(crate) fn code_del_into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("del".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame
    }

    /// # apply() 函数
    ///
    /// 应用Del命令，并将响应写入到Connection实例
    pub(crate) async fn apply(
        self,
        db: &Database,
        connection: &mut Connection,
    ) -> crate::Result<()> {
        db.del(&self.key);

        // 如果del_num为0，那么说明键不存在
        let response = Frame::Simple("OK".to_string());
        debug!(?response);

        // 往连接中写入响应
        connection.write_frame(&response).await?;

        Ok(())
    }
}
