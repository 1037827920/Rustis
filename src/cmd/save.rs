//! save命令的实现

use crate::networking::{connection::Connection, frame::Frame};
use bytes::Bytes;
use tracing::{debug, instrument};

use super::Database;

/// # Save 结构体
///
/// 进行一次RBD快照
///
/// # 语法
///
/// save
#[derive(Debug)]
pub struct Save;

impl Save {
    pub(crate) fn new() -> Save {
        Save
    }

    /// # decode_save_from_frame() 函数
    ///
    /// 将帧解码为save命令
    pub(crate) fn decode_save_from_frame() -> crate::Result<Save> {
        Ok(Save::new())
    }

    /// # code_save_into_frame() 函数
    ///
    /// 将save命令编码为帧
    pub(crate) fn code_save_into_frame(&self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("save".as_bytes()));
        frame
    }

    /// # apply() 函数
    ///
    /// 应用save命令，并将响应写入到Connection实例
    #[instrument(skip(self, db, connection))]
    pub(crate) async fn apply(
        self,
        db: &Database,
        connection: &mut Connection,
    ) -> crate::Result<()> {
        // 保存到RDB文件，文件名先写死了
        db.save_to_rdb("rustis.rdb")?;

        // 服务器响应
        let response = Frame::Simple("OK".to_string());
        debug!(?response);

        // 写入响应
        connection.write_frame(&response).await?;

        Ok(())
    }
}
