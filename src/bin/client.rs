//! Rustis客户端

use bytes::Bytes;
use clap::Parser;
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    terminal::{disable_raw_mode, enable_raw_mode},
};
use rustis::{client::Client, DEFAULT_PORT};
use tracing::{span, Level};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};
use std::{
    fs::File, io::{stdout, Write}, str, sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    }
};
use tokio::{
    sync::{broadcast, mpsc, Mutex},
    time::Duration,
};

#[derive(Parser, Debug)]
#[command(name = "rustis-client", version, author, about = "rustis client")]
struct Cli {
    #[arg(id = "hostname", long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,
}

#[tokio::main]
async fn main() -> rustis::Result<()> {
    // 如果不存在logs文件夹，则创建
    if !std::path::Path::new("logs").exists() {
        std::fs::create_dir("logs").expect("无法创建logs文件夹");
    }
    // 输出到文件中
    let file = File::create("logs/client.log").expect("无法创建日志文件");
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
    let main_span = span!(Level::DEBUG, "client-main");

    // 解析命令行参数
    let cli = Cli::parse();

    // 获取远程连接地址以及端口
    let addr = format!("{}:{}", cli.host, cli.port);

    // 创建一个客户端
    let mut client = Client::connect(&addr).await?;
    // 客户端是否处于订阅模式
    let is_subscription_mode = Arc::new(AtomicBool::new(false));
    // 创建命令历史管理器
    let command_history = CommandHistory::default();

    // 创建广播通道用于处理Ctrl+C信号
    let (shutdown_tx, _) = broadcast::channel(1);
    let mut shutdown_rx = shutdown_tx.subscribe();

    // 创建一个channel用于接收命令
    let (tx, mut rx) = mpsc::channel::<String>(100);
    // 创建输入任务
    let input_task = tokio::spawn({
        let tx = tx.clone();
        let history = command_history.clone();
        let shutdown_tx = shutdown_tx.clone();
        let is_subscription_mode = is_subscription_mode.clone();
        async move {
            loop {
                match read_line(history.clone(), shutdown_tx.clone()).await {
                    Ok(line) => {
                        if tx.send(line).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => {
                        // DEBUG
                        // println!("is_subscription_mode: {}", is_subscription_mode.load(Ordering::SeqCst));
                        if !is_subscription_mode.load(Ordering::SeqCst) {
                            break;
                        }
                    }
                }
            }
        }
    });

    tracing::event!(parent: &main_span, Level::DEBUG, "Rustis client has been started");
    println!("\rRustis client has been started");

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                println!("\nReceived interrupt. Exiting...");
                input_task.abort(); // 中断输入任务
                break;
            }
            input = rx.recv() => {
                if let Some(input) = input {
                    // 去除换行符
                    let input = input.trim().to_string();

                    // 如果输入为空，则继续
                    if input.is_empty() {
                        continue;
                    }

                    // 将命令添加到历史记录
                    command_history.add(&input).await;

                    // 解析输入命令
                    match parse_command(&input) {
                        Ok(command) => {
                            match command {
                                Command::Get { key } => {
                                    if let Some(value) = client.get(&key).await? {
                                        if let Ok(string) = str::from_utf8(&value) {
                                            println!("\r\"{}\"", string);
                                        } else {
                                            println!("\r{:?}", value);
                                        }
                                    } else {
                                        // \r打断前面的>输出
                                        println!("\r(nil)");
                                    }
                                }
                                Command::Ping { msg } => {
                                    let value = client.ping(msg).await?;
                                    if let Ok(string) = str::from_utf8(&value) {
                                        println!("\r\"{}\"", string);
                                    } else {
                                        println!("\r{:?}", value);
                                    }
                                }
                                Command::Publish { channel, message } => {
                                    client.publish(&channel, message).await?;
                                    // \r打断前面的>输出
                                    println!("\rPublish Ok");
                                }
                                Command::Set { key, value, expire } => {
                                    match expire {
                                        Some((ExpireMode::EX, seconds)) => {
                                            let duration = Duration::from_secs(seconds);
                                            client.set_with_expires(&key, value, duration).await?;
                                        }
                                        Some((ExpireMode::PX, milliseconds)) => {
                                            let duration = Duration::from_millis(milliseconds);
                                            client.set_with_expires(&key, value, duration).await?;
                                        }
                                        None => {
                                            client.set(&key, value).await?;
                                        }
                                    }
                                    // \r打断前面的>输出
                                    println!("\rSet Ok");
                                }
                                Command::Subscribe { channels } => {
                                    if channels.is_empty() {
                                        return Err("\rchannel(s) must be provided".into());
                                    }
                                    let mut subscriber = client.subscribe(channels).await?;

                                    // 等待消息
                                    is_subscription_mode.store(true, Ordering::SeqCst);
                                    loop {
                                        tokio::select! {
                                            msg = subscriber.next_message() => {
                                                match msg {
                                                    Ok(Some(msg)) => {
                                                        println!(
                                                            "\rgot message from the channel: {}; message = {:?}",
                                                            msg.channel, msg.content
                                                        );
                                                    }
                                                    Ok(None) => break,
                                                    Err(e) => {
                                                        eprintln!("Error receiving message: {:?}", e);
                                                        break;
                                                    }
                                                }
                                            }
                                            _ = shutdown_rx.recv() => {
                                                // 取消所有订阅
                                                subscriber.unsubscribe(&[]).await?;
                                                // 发送退出订阅者模式的信号给服务器
                                                subscriber.exit_subscribe().await?;
                                                println!("\rReceived interrupt. Exiting subscription mode...");
                                                break;
                                            }
                                            input = rx.recv() => {
                                                if let Some(input) = input {
                                                    // 去除换行符
                                                    let input = input.trim().to_string();

                                                    // 如果输入为空，则继续
                                                    if input.is_empty() {
                                                        continue;
                                                    }

                                                    // 解析输入命令
                                                    match parse_command(&input) {
                                                        Ok(command) => {
                                                            match command {
                                                                Command::Subscribe { channels } => {
                                                                    // 新增订阅的channel
                                                                    subscriber.subscribe(&channels).await?;
                                                                }
                                                                Command::Unsubscribe { channels } => {
                                                                    // 取消订阅的channel
                                                                    subscriber.unsubscribe(&channels).await?;
                                                                }
                                                                _ => {
                                                                    println!("\rUnsupported command in subscription mode");
                                                                }
                                                            };
                                                        }
                                                        Err(e) => {
                                                            println!("{e}");
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    // 从subscriber中取回Client对象所有权
                                    client = subscriber.into_client();
                                    is_subscription_mode.store(false, Ordering::SeqCst);
                                }
                                Command::Unsubscribe { channels } => {
                                    // 在客户端模式下，取消订阅是不支持的
                                    println!("\rUnsubscribe is unsupported in client mode. channels: {:?}", channels);
                                }
                                Command::Save {} => {
                                    client.save().await?;
                                    // \r打断前面的>输出
                                    println!("\rSave Ok");
                                }
                                Command::Del { key } => {
                                    client.del(&key).await?;
                                    // \r打断前面的>输出
                                    println!("\rDel Ok");
                                }
                            };
                        }
                        Err(e) => {
                            println!("{e}");
                        }
                    }
                } else {
                    break;
                }
            }
        }
    }

    // 恢复光标显示
    print!("\x1B[?25h");
    disable_raw_mode()?;

    Ok(())
}

#[derive(Debug)]
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
        expire: Option<(ExpireMode, u64)>,
    },
    Subscribe {
        channels: Vec<String>,
    },
    #[allow(dead_code)]
    Unsubscribe {
        channels: Vec<String>,
    },
    Save {},
    Del {
        key: String,
    }
}

#[derive(Debug, Clone)]
enum ExpireMode {
    EX, // 秒
    PX, // 毫秒
}

fn parse_command(input: &str) -> Result<Command, String> {
    let parts: Vec<&str> = input.split_whitespace().collect();

    if parts.is_empty() {
        return Err("\rEmpty command".to_string());
    }

    match parts[0].to_lowercase().as_str() {
        "get" => {
            if parts.len() != 2 {
                // \r打断前面的>输出
                return Err("\r'get' command usage: get <key>".to_string());
            }
            Ok(Command::Get {
                key: parts[1].to_string(),
            })
        }
        "ping" => {
            let msg = if parts.len() > 1 {
                Some(Bytes::from(parts[1..].join(" ").into_bytes()))
            } else {
                None
            };
            Ok(Command::Ping { msg })
        }
        "publish" => {
            if parts.len() < 3 {
                return Err("\r'publish' command usage: publish <channel> <message>".to_string());
            }
            Ok(Command::Publish {
                channel: parts[1].to_string(),
                message: Bytes::from(parts[2..].join(" ").into_bytes()),
            })
        }
        "set" => {
            if parts.len() < 3 {
                return Err("\r'set' command usage: set <key> <value>".to_string());
            }

            let key = parts[1].to_string();
            let value = Bytes::from(parts[2].as_bytes().to_vec());

            // 处理可选的过期参数
            let expire = if parts.len() > 3 {
                match parts[3].to_lowercase().as_str() {
                    "ex" if parts.len() == 5 => parts[4]
                        .parse::<u64>()
                        .map(|seconds| Some((ExpireMode::EX, seconds)))
                        .map_err(|_| "Invalid expire time for EX".to_string()),
                    "px" if parts.len() == 5 => parts[4]
                        .parse::<u64>()
                        .map(|milliseconds| Some((ExpireMode::PX, milliseconds)))
                        .map_err(|_| "Invalid expire time for PX".to_string()),
                    _ => Ok(None),
                }
            } else {
                Ok(None)
            }?;

            Ok(Command::Set { key, value, expire })
        }
        "subscribe" => {
            if parts.len() < 2 {
                return Err("\r'subscribe' command usage: subscribe <channel(s)>".to_string());
            }
            Ok(Command::Subscribe {
                channels: parts[1..].iter().map(|&s| s.to_string()).collect(),
            })
        }
        "unsubscribe" => {
            if parts.len() < 2 {
                return Err("\r'unsubscribe' command usage: unsubscribe <channel(s)>".to_string());
            }
            Ok(Command::Unsubscribe {
                channels: parts[1..].iter().map(|&s| s.to_string()).collect(),
            })
        }
        "save" => Ok(Command::Save {}),
        "del" => {
            if parts.len() != 2 {
                // \r打断前面的>输出
                return Err("\r'del' command usage: del <key>".to_string());
            }
            Ok(Command::Del {
                key: parts[1].to_string(),
            })
        }
        _ => Err(format!("\rUnknown command: {}", parts[0])),
    }
}

// 命令历史管理器
#[derive(Default, Clone)]
struct CommandHistory {
    history: Arc<Mutex<Vec<String>>>,
    current_index: Arc<Mutex<Option<usize>>>,
}

impl CommandHistory {
    // 添加新命令到历史记录
    async fn add(&self, command: &str) {
        let mut history = self.history.lock().await;
        // 避免重复添加最近的命令
        if history.last().map_or(true, |last| last != command) {
            history.push(command.to_string());
        }
        *self.current_index.lock().await = None;
    }

    // 获取上一条命令
    async fn prev(&self) -> Option<String> {
        let history = self.history.lock().await;
        let mut current_index = self.current_index.lock().await;

        if history.is_empty() {
            return None;
        }

        *current_index = match *current_index {
            None => Some(history.len() - 1),
            Some(0) => Some(0),
            Some(idx) => Some(idx - 1),
        };

        current_index.map(|idx| history[idx].clone())
    }

    // 获取下一条命令
    async fn next(&self) -> Option<String> {
        let history = self.history.lock().await;
        let mut current_index = self.current_index.lock().await;

        if history.is_empty() {
            return None;
        }

        *current_index = match *current_index {
            None => None,
            Some(idx) if idx + 1 < history.len() => Some(idx + 1),
            Some(_) => None,
        };

        current_index.map(|idx| history[idx].clone())
    }
}

// 自定义输入处理函数
async fn read_line(
    history: CommandHistory,
    shutdown_tx: broadcast::Sender<()>,
) -> Result<String, std::io::Error> {
    enable_raw_mode()?;

    let mut input = String::new();
    let mut cursor_pos = 0;

    // 隐藏光标
    print!("\x1B[?25l");

    loop {
        // 清除当前行并重新绘制提示符和输入内容
        print!("\r> ");
        for (i, c) in input.chars().enumerate() {
            if i == cursor_pos {
                print!("\x1B[7m{}\x1B[0m", c); // 高亮当前光标位置
            } else {
                print!("{}", c);
            }
        }
        // 如果光标在输入的末尾，显示一个高亮的空格
        if cursor_pos == input.len() {
            print!("\x1B[7m \x1B[0m");
        }
        // 清除光标后的内容
        print!("\x1B[K");
        stdout().flush()?;

        // 这里一开始用的阻塞，因为每次输出响应后都会阻塞直到下一次输入才会运行上面的代码，所以这里改成非阻塞
        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key_event) = event::read()? {
                match key_event {
                    KeyEvent {
                        code: KeyCode::Char(c),
                        modifiers: KeyModifiers::NONE,
                        ..
                    } => {
                        input.insert(cursor_pos, c);
                        cursor_pos += 1;
                    }
                    KeyEvent {
                        code: KeyCode::Backspace,
                        ..
                    } if !input.is_empty() && cursor_pos > 0 => {
                        input.remove(cursor_pos - 1);
                        cursor_pos -= 1;
                    }
                    KeyEvent {
                        code: KeyCode::Left,
                        ..
                    } if cursor_pos > 0 => {
                        cursor_pos -= 1;
                    }
                    KeyEvent {
                        code: KeyCode::Right,
                        ..
                    } if cursor_pos < input.len() => {
                        cursor_pos += 1;
                    }
                    KeyEvent {
                        code: KeyCode::Up, ..
                    } => {
                        // 获取上一条历史命令
                        if let Some(prev_cmd) = history.prev().await {
                            input = prev_cmd;
                            cursor_pos = input.len();
                        }
                    }
                    KeyEvent {
                        code: KeyCode::Down,
                        ..
                    } => {
                        // 获取下一条历史命令
                        if let Some(next_cmd) = history.next().await {
                            input = next_cmd;
                            cursor_pos = input.len();
                        } else {
                            input.clear();
                            cursor_pos = 0;
                        }
                    }
                    KeyEvent {
                        code: KeyCode::Enter,
                        ..
                    } => {
                        // 清除当前行
                        print!("\r> ");
                        for c in input.chars() {
                            print!("{}", c);
                        }
                        print!("\x1B[K"); // 清除光标后的内容
                        stdout().flush()?;

                        disable_raw_mode()?;
                        println!();
                        return Ok(input);
                    }
                    KeyEvent {
                        code: KeyCode::Char('c'),
                        modifiers: KeyModifiers::CONTROL,
                        ..
                    } => {
                        // 清除当前行
                        print!("\r> ");
                        for c in input.chars() {
                            print!("{}", c);
                        }
                        print!("\x1B[K"); // 清除光标后的内容
                        stdout().flush()?;

                        disable_raw_mode()?;
                        println!("^C");
                        // 发送关闭信号
                        let _ = shutdown_tx.send(());
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Interrupted,
                            "Ctrl+C pressed",
                        ));
                    }
                    _ => {}
                }
            }
        }
    }
}
