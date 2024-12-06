//! 服务器运行命令的实现

use clap::Parser;
use rustis::{server::run, DEFAULT_PORT};
use tokio::{net::TcpListener, signal};

#[tokio::main]
async fn main() -> rustis::Result<()> {
    set_up_subcriber()?;

    let cli = Cli::parse();
    let port = cli.port.unwrap_or(DEFAULT_PORT);

    // 绑定到指定端口
    let listener = TcpListener::bind(&format!("localhost:{port}")).await?;

    // 运行服务器
    run(listener, signal::ctrl_c()).await;

    Ok(())
}

#[cfg(not(feature = "otel"))]
fn set_up_subcriber() -> rustis::Result<()> {
    tracing_subscriber::fmt::try_init()
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
