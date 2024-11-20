//! Unknown命令, 用于处理未知的命令

use tracing::debug;

use crate::networking::{connection::Connection, frame::Frame};

#[derive(Debug)]
pub(crate) struct Unknown {
    command_name: String,
}

impl Unknown {
    /// # new() 函数
    ///
    /// 用于创建一个响应来自客户端的未知命令
    pub(crate) fn new(key: impl ToString) -> Self {
        Self {
            command_name: key.to_string(),
        }
    }

    /// # get_name() 函数
    ///
    /// 获取命令名称
    pub(crate) fn get_name(&self) -> &str {
        &self.command_name
    }

    /// # apply() 函数
    ///
    /// 应用命令，响应客户端指示命令不可识别
    pub(crate) async fn apply(&self, connection: &mut Connection) -> crate::Result<()> {
        let response = Frame::Error(format!("未知命令: {}", self.command_name));

        // ?将使用Debug trait来格式化值，而不是使用Display trait
        debug!(?response);

        connection.write_frame(&response).await?;

        Ok(())
    }
}
