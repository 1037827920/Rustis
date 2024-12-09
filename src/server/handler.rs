//! Handler结构体的实现，处理每个来自客户端的连接

use tokio::sync::mpsc;
use tracing::{debug, instrument};

use crate::cmd::Command;
use crate::networking::connection::Connection;
use crate::persistence::database::Database;

use super::shutdown::Shutdown;

// use crate::persistence::db::Db;

/// # 结构体功能
///
/// 处理每个连接，从连接中读取请求并将命令应用到db
#[derive(Debug)]
pub(super) struct Handler {
    /// 共享数据库
    database: Database,
    /// 连接
    connection: Connection,
    /// 监听服务器关闭信号
    shutdown: Shutdown,
    /// 只作为一个标记，当Handler实例drop后，说明Handler已经关闭
    _shutdown_finish_tx: mpsc::Sender<()>,
}

impl Handler {
    /// # new() 函数
    ///
    /// 创建一个新的Handler实例
    pub fn new(
        database: Database,
        connection: Connection,
        shutdown: Shutdown,
        _shutdown_finish_tx: mpsc::Sender<()>,
    ) -> Self {
        Self {
            database,
            connection,
            shutdown,
            _shutdown_finish_tx,
        }
    }

    /// # run() 函数
    ///
    /// 处理一个连接，从socket中读取并处理请求帧，将响应写回套接字
    ///
    /// 目前没有实现pipeline，pipeline是指在没有交叉帧的情况下， 每个连接同时处理多个请求的能力，更多信息请访问：https://redis.io/topics/pipelining
    ///
    /// 当接收到关闭信号时，连接被处理，直到它达到安全状态，此时它被终止。
    #[instrument]
    pub(super) async fn run(&mut self) -> crate::Result<()> {
        // 只要没有收到关闭信号，就一直尝试读取新的请求帧
        while !self.shutdown.is_shutdown() {
            // 读取请求帧的同时监听关闭信号（通过select!来执行其中一个任务）
            let frame = tokio::select! {
                frame = self.connection.read_frame() => frame?,
                _ = self.shutdown.receiving() => {
                    // 收到关闭信号，直接返回
                    return Ok(());
                }
            };

            let frame = match frame {
                Some(frame) => frame,
                None => return Ok(()), // 缓冲区已经没有数据了，直接返回
            };

            let cmd = Command::decode_cmd_from_frame(frame)?;

            // ?表示用Debug trait打印出错误信息，而不是Display trait
            debug!(?cmd);

            cmd.apply(&self.database, &mut self.connection, &mut self.shutdown)
                .await?;
        }

        Ok(())
    }
}
