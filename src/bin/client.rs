//! Rustis客户端

use bytes::Bytes;
use clap::Parser;
use rustis::{client::Client, DEFAULT_PORT};
use std::{io::Write, num::ParseIntError, str, time::Duration};
use tokio::{
    io::{self, AsyncBufReadExt, BufReader},
    signal,
    sync::mpsc,
};

#[tokio::main]
async fn main() -> rustis::Result<()> {
    // 初始化日志系统
    set_up_subcriber()?;

    // 解析命令行参数
    let cli = Cli::parse();

    // 获取远程连接地址以及端口
    let addr = format!("{}:{}", cli.host, cli.port);

    // 创建一个客户端
    let mut client = Client::connect(&addr).await?;

    // 创建一个任务来监听ctrl+c信号
    tokio::spawn(async move {
        signal::ctrl_c().await.unwrap();
        println!();
        std::process::exit(0);
    });

    // 创建一个channel用于接收命令
    let (tx, mut rx) = mpsc::channel::<String>(100);
    // 创建一个任务来读取用户输入
    let input_tx = tx.clone();
    tokio::spawn(async move {
        let stdin = io::stdin();
        let reader = BufReader::new(stdin);
        let mut lines = reader.lines();

        while let Ok(Some(line)) = lines.next_line().await {
            if input_tx.send(line).await.is_err() {
                break;
            }
        }
    });

    println!("Rusttis client has been started");

    loop {
        // 打印提示符
        print!("> ");
        std::io::stdout().flush().unwrap();
        if let Some(input) = rx.recv().await {
            // 去除换行符
            let input = input.trim().to_string();

            // 如果输入为空，则继续
            if input.is_empty() {
                continue;
            }

            // 将输入拆分为命令和参数
            // clap在解析命令参数时，会默认第一个元素是程序的名称，即argv[0]
            let args = std::iter::once("rustis-client").chain(input.split_whitespace());

            // 解析输入命令
            match Command::try_parse_from(args) {
                Ok(command) => {
                    match command {
                        Command::Get { key } => {
                            if let Some(value) = client.get(&key).await? {
                                if let Ok(string) = str::from_utf8(&value) {
                                    println!("\"{}\"", string);
                                } else {
                                    println!("{:?}", value);
                                }
                            } else {
                                println!("(nil)");
                            }
                        }
                        Command::Ping { msg } => {
                            let value = client.ping(msg).await?;
                            if let Ok(string) = str::from_utf8(&value) {
                                println!("\"{}\"", string);
                            } else {
                                println!("{:?}", value);
                            }
                        }
                        Command::Publish { channel, message } => {
                            client.publish(&channel, message).await?;
                            println!("Publish Ok");
                        }
                        Command::Set {
                            key,
                            value,
                            expries: None,
                        } => {
                            client.set(&key, value).await?;
                            println!("Set Ok");
                        }
                        Command::Set {
                            key,
                            value,
                            expries: Some(expries),
                        } => {
                            client.set_with_expires(&key, value, expries).await?;
                            println!("Set Ok");
                        }
                        Command::Subscribe { channels } => {
                            if channels.is_empty() {
                                return Err("channel(s) must be provided".into());
                            }
                            let mut subscriber = client.subscribe(channels).await?;

                            // 等待消息
                            while let Some(msg) = subscriber.next_message().await? {
                                println!(
                                    "got message from the channel: {}; message = {:?}",
                                    msg.channel, msg.content
                                );
                            }

                            // 从subscriber中取回Client对象所有权
                            client = subscriber.into_client();
                        }
                    };
                }
                Err(e) => {
                    println!("解析命令错误: {}", e);
                }
            }
        } else {
            break;
        }
    }

    Ok(())
}

fn set_up_subcriber() -> rustis::Result<()> {
    tracing_subscriber::fmt::try_init()
}

#[derive(Parser, Debug)]
#[command(name = "rustis-client", version, author, about = "rustis client")]
struct Cli {
    #[arg(id = "hostname", long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,
}

#[derive(Debug, Parser)]
enum Command {
    Get {
        key: String,
    },
    Ping {
        msg: Option<Bytes>,
    },
    Publish {
        channel: String,
        message: Bytes,
    },
    Set {
        key: String,
        value: Bytes,
        #[arg(value_parser = duration_from_ms_str)]
        expries: Option<Duration>,
    },
    Subscribe {
        channels: Vec<String>,
    },
}

fn duration_from_ms_str(s: &str) -> Result<Duration, ParseIntError> {
    let ms = s.parse::<u64>()?;
    Ok(Duration::from_millis(ms))
}
