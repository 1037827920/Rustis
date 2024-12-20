mod handler;
mod listener;
pub mod shutdown;

use std::future::Future;
use tokio::{
    net::TcpListener,
    sync::{broadcast, mpsc},
};
use tracing::{debug, error, info, instrument};

use listener::Listener;

use crate::persistence::database::DatabaseWrapper;

/// # run() 函数
///
/// 运行服务器，暴露给crate外的接口
#[instrument(skip(listener, shutdown))]
pub async fn run(listener: TcpListener, shutdown: impl Future, is_load_rdb: bool) {
    // 创建一个广播channel，用来通知所有handler关闭信号
    // Receiver在需要时才创建，通过调用Sender的subscriber()方法创建
    // 当handler收到关闭信号后，会把自己的is_shutdown设置为true，退出handle的run循环
    let (shutdown_tx, _) = broadcast::channel(1);
    // 创建一个多生产者单消费者channel, 当所有的生产者drop后，channel就会被关闭，说明所有的handler已经关闭，这时候可以优雅地关闭服务器了
    let (shutdown_finish_tx, mut shutdown_finish_rx) = mpsc::channel(1);

    // 初始化Listener
    let mut server = Listener::new(
        DatabaseWrapper::new(is_load_rdb),
        listener,
        shutdown_tx,
        shutdown_finish_tx,
    );

    // 同时运行服务器和监听关闭信号
    tokio::select! {
        ret = server.run() => {
            if let Err(err) = ret {
                // %表示使用Display trait格式化err
                error!(cause = %err, "Server error");
            }
        }
        _ = shutdown => {
            info!("Has received shutdown signal");
        }
    }

    // 关机前进行一次RDB快照
    debug!("Save to RDB before shutdown");
    server.save_rdb().expect("Failed to save RDB");

    // 通过解构赋值从server中取出shutdown_tx, shutdown_finish_tx
    let Listener {
        shutdown_tx,
        shutdown_finish_tx,
        ..
    } = server;
    // 先Drop掉shutdown_tx，shutdown_finish_tx
    drop(shutdown_tx);
    drop(shutdown_finish_tx);

    // 等待所有的handler关闭
    let _ = shutdown_finish_rx.recv().await;
}
