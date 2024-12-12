//! 服务器运行命令的实现

use std::fs::File;

use clap::Parser;
use rustis::{server::run, DEFAULT_PORT};
use tokio::{net::TcpListener, signal};
use tracing::{event, span, Level};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> rustis::Result<()> {
    // 如果不存在logs文件夹，则创建
    if !std::path::Path::new("logs").exists() {
        std::fs::create_dir("logs").expect("无法创建logs文件夹");
    }
    // 输出到文件中
    let file = File::create("logs/server.log").expect("无法创建日志文件");
    let (non_blocking_appender, _guard) = tracing_appender::non_blocking(file);
    let file_layer = fmt::layer()
        .with_timer(fmt::time::UtcTime::rfc_3339()) // 使用 RFC 3339 格式的 UTC 时间
        .with_target(true) // 显示日志目标
        .with_level(true) // 显示日志级别
        .with_line_number(true) // 显示行号
        .with_ansi(false)
        .with_writer(non_blocking_appender)
        .compact();

    // 初始化全局subscriber
    tracing_subscriber::registry().with(file_layer).init();

    // 创建一个root span
    let main_span = span!(Level::DEBUG, "server-main");

    let cli = Cli::parse();
    let port = cli.port.unwrap_or(DEFAULT_PORT);

    // 绑定到指定端口
    let listener = TcpListener::bind(&format!("localhost:{port}")).await?;

    event!(parent: &main_span, Level::DEBUG, "Rustis server has been started on port {port}");
    // 运行服务器
    run(listener, signal::ctrl_c(), true).await;

    Ok(())
}

#[derive(Parser, Debug)]
#[command(
    name = "rust-redis-server",
    version,
    author,
    about = "rust redis server"
)]
struct Cli {
    // 使用了clap crate的#[arg]宏
    // 指定了long参数，long是指以两个连字符(--)开头的参数
    #[arg(long)]
    port: Option<u16>,
}
