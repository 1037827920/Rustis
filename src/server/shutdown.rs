//! Shutdown结构体，监听服务器关闭信号

use tokio::sync::broadcast;

/// # 结构体功能
/// 
/// 监听服务器关闭信号，最常见的通知各个部分关闭的方式就是使用一个广播channel，然后任务都持有Receiver，等待Sender发送关闭信号。
#[derive(Debug)]
pub struct Shutdown {
    /// Receiver, 用于接收关闭信号
    receiver: broadcast::Receiver<()>,
    /// 标识符，用于标识是否关闭
    is_shutdown: bool,
}

impl Shutdown {
    pub fn new(receiver: broadcast::Receiver<()>) -> Self {
        Self {
            receiver,
            is_shutdown: false,
        }
    }

    pub fn is_shutdown(&self) -> bool{
        self.is_shutdown
    }

    /// # 函数功能 
    /// 
    /// 等待关闭信号
    pub async fn receiving(&mut self) {
        if self.is_shutdown { // 如果is_shutdown为true，说明已经关闭了
            return;
        }

        // 阻塞等待关闭信号
        let _ = self.receiver.recv().await;

        // 设置is_shutdown为true
        self.is_shutdown = true;
    }
}