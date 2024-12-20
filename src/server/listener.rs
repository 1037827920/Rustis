//! Listener结构体的实现，监听来自客户端的连接

use tokio::{
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc},
    time::{self, Duration},
};
use tracing::{error, info, instrument};

use crate::{
    networking::connection::Connection, persistence::database::DatabaseWrapper,
    server::shutdown::Shutdown,
};

use super::handler::Handler;

/// 监听来自客户端连接
#[derive(Debug)]
pub(super) struct Listener {
    /// Database实例的包装器，是为了在实例被删除时，通过后天清除任务发出关闭的信号，允许有序地清理database
    database_wrapper: DatabaseWrapper,
    /// TCP监听器
    listener: TcpListener,
    /// 关闭信号发送者
    pub shutdown_tx: broadcast::Sender<()>,
    /// 只作为一个标记，传递给Handler
    pub shutdown_finish_tx: mpsc::Sender<()>,
}

impl Listener {
    /// # new() 函数
    ///
    /// 创建一个新的Listener实例
    pub fn new(
        database_wrapper: DatabaseWrapper,
        listener: TcpListener,
        shutdown_tx: broadcast::Sender<()>,
        shutdown_finish_tx: mpsc::Sender<()>,
    ) -> Self {
        Self {
            database_wrapper,
            listener,
            shutdown_tx,
            shutdown_finish_tx,
        }
    }

    /// # 函数功能
    ///
    /// 接收入站连接
    ///
    /// # 错误处理
    ///
    /// 采用指数退避的方式解决重试问题，每次指数增长请求之间的间隔时间，直到达到最大重试次数，返回错误
    async fn accept(&mut self) -> crate::Result<TcpStream> {
        // 每次重试请求之间的等待时间
        let mut backoff = 1;

        // 尝试去接受连接
        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(error) => {
                    if backoff > 64 {
                        // 重试次数太多了，返回错误
                        return Err(error.into());
                    }
                }
            }

            // 睡眠一段时间
            time::sleep(Duration::from_secs(backoff)).await;

            // 指数增长
            backoff *= 2;
        }
    }

    /// # 函数功能
    ///
    /// 监听入站连接，对于每个入站连接，生成一个任务来处理连接
    #[instrument(skip(self))]
    pub(super) async fn run(&mut self) -> crate::Result<()> {
        info!("waiting for incoming connections");

        loop {
            // 尝试接受连接，获取socket
            let socket = self.accept().await?;

            let mut handler = Handler::new(
                self.database_wrapper.database(),
                Connection::new(socket),
                Shutdown::new(self.shutdown_tx.subscribe()),
                self.shutdown_finish_tx.clone(),
            );

            // 生成一个任务来处理连接
            tokio::spawn(async move {
                // 处理连接
                if let Err(err) = handler.run().await {
                    error!(cause = ?err, "处理连接时发生错误");
                }
            });
        }
    }

    pub(crate) fn save_rdb(&self) -> crate::Result<()> {
        self.database_wrapper.database().save_to_rdb("rustis.rdb")
    }
}
