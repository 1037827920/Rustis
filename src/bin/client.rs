
use core::str;
use std::collections::btree_map::Keys;

use clap::{Parser, Subcommand};
use rust_redis::{ DEFAULT_PORT, client::Client};
use bytes::Bytes;

#[tokio::main]
async fn main() -> rust_redis::Result<()> {
    // 初始化日志系统
    set_up_subcriber()?;

    // 解析命令行参数
    let cli = Cli::parse();

    // 获取远程连接地址以及端口
    let addr = format!("{}:{}", cli.host, cli.port);

    // 创建一个redis客户端
    let mut client = Client::connect(&addr).await?;

    // 处理请求命令
    // match cli.cmd {
    //     Command::Ping { msg } => {
    //         let value = client.ping(msg).await?;
    //         if let Ok(string) = str::from_utf8(&value) {
    //             println!("\"{}\"", string);
    //         } else {
    //             println!("{:?}", value);
    //         }
    //     }
    //     Command::Set { key, value } => {
    //         client.set(&key, value).await?;
    //         println!("OK");
    //     }
    // }

    Ok(())
}

fn set_up_subcriber() -> rust_redis::Result<()> {
    tracing_subscriber::fmt::try_init()
}

#[derive(Parser, Debug)]
#[command(name = "rust-redis-client", version, author, about = "rust redis client")]
struct Cli {
    #[clap(subcommand)]
    cmd: Command,

    #[arg(id = "hostname", long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,
}

#[derive(Subcommand, Debug)]
enum Command {
    Ping {
        /// 给ping的消息
        msg: Option<Bytes>,
    },
    /// 获取键值
    // Get {
    //     /// 键
    //     key: String,
    // },
    /// 设置键值
    Set {
        /// 键
        key: String,
        /// 值
        value: Bytes,
    },
}