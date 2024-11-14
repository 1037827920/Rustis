//! Listener结构体的实现，监听来自客户端的连接

use tokio::{net::{TcpListener, TcpStream}, time::{self, Duration}};
use tracing::info;


/// 监听来自客户端连接
struct Listener {
    /// TCP监听器
    listener: TcpListener,
}

impl Listener {
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
    async fn run(&mut self) -> crate::Result<()> {
        info!("接受入站连接...");

        loop {
            // 尝试接受连接，获取socket
            let socket = self.accept().await?;
        }

        Ok(())
    }
}