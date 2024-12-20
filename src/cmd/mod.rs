pub mod get;
pub mod ping;
pub mod publish;
pub mod save;
pub mod set;
pub mod subscribe;
pub mod del;
mod unknown;

use crate::{
    networking::{connection::Connection, frame::Frame, parse::Parse},
    persistence::database::Database,
    server::shutdown::Shutdown,
};
use get::Get;
use ping::Ping;
use publish::Publish;
use save::Save;
use set::Set;
use del::Del;
use subscribe::{ExitSubscribe, Subscribe, Unsubscribe};
use tracing::instrument;
use unknown::Unknown;

#[derive(Debug)]
pub enum Command {
    /// # Get 命令
    ///
    /// 获取key的值
    Get(Get),
    /// # Set 命令
    ///
    /// 从key映射到value，如果key已经映射到了一个值，那么旧值将被替换
    Set(Set),
    /// # Publish 命令
    ///
    /// 将消息发布到指定的频道
    Publish(Publish),
    /// # Subscribe 命令
    ///
    /// 订阅一个或多个频道
    Subscribe(Subscribe),
    /// # Unsubscribe 命令
    ///
    /// 退订一个或多个频道
    Unsubscribe(Unsubscribe),
    /// # ExitSubscribe 命令
    ///
    /// 退出订阅
    ExitSubscribe(ExitSubscribe),
    /// # Ping 命令
    ///
    /// 检查服务器是否存活
    Ping(Ping),
    /// # Save 命令
    ///
    /// 保存数据库到RDB文件
    Save(Save),
    /// # Del 命令
    /// 
    /// 删除key
    Del(Del),
    /// # Unknown命令
    ///
    /// 未知命令
    Unknown(Unknown),
}

impl Command {
    /// # get_name() 函数
    ///
    /// 返回命令的名称
    pub(crate) fn get_name(&self) -> &str {
        match self {
            Command::Set(_) => "set",
            Command::Unknown(cmd) => cmd.get_name(),
            Command::Get(_) => "get",
            Command::Publish(_) => "publish",
            Command::Subscribe(_) => "subscribe",
            Command::Unsubscribe(_) => "unsubscribe",
            Command::ExitSubscribe(_) => "exitsubscribe",
            Command::Ping(_) => "ping",
            Command::Save(_) => "save",
            Command::Del(_) => "del",
        }
    }

    /// # decode_cmd_from_frame() 函数
    ///
    /// 从数据帧中解码出命令
    pub(crate) fn decode_cmd_from_frame(frame: Frame) -> crate::Result<Command> {
        // 帧必须是Array帧
        let mut parse = Parse::new(frame)?;

        // 获取命令名称，要将其转换为小写
        let cmd_name = parse.next_string()?.to_lowercase();

        // 模式匹配命令
        let cmd = match &cmd_name[..] {
            "get" => Command::Get(Get::decode_get_from_frame(&mut parse)?),
            "ping" => Command::Ping(Ping::decode_ping_from_frame(&mut parse)?),
            "publish" => Command::Publish(Publish::decode_publish_from_frame(&mut parse)?),
            "set" => Command::Set(Set::decode_set_from_frame(&mut parse)?),
            "subscribe" => Command::Subscribe(Subscribe::decode_subscribe_from_frame(&mut parse)?),
            "unsubscribe" => {
                Command::Unsubscribe(Unsubscribe::decode_unsubscribe_from_frame(&mut parse)?)
            }
            "exitsubscribe" => {
                Command::ExitSubscribe(ExitSubscribe::decode_exit_subscribe_from_frame(&mut parse)?)
            }
            "save" => Command::Save(Save::decode_save_from_frame()?),
            "del" => Command::Del(Del::decode_del_from_frame(&mut parse)?),
            _ => {
                // 如果命令未知，那么返回Unknown命令
                return Ok(Command::Unknown(Unknown::new(cmd_name)));
            }
        };

        // 检查parse中是否有未使用的帧
        parse.is_finish()?;

        // 返回命令
        Ok(cmd)
    }

    /// # apply() 函数
    ///
    /// 对特定数据库应用命令，这函数由服务器执行
    #[instrument(skip(self, database, connection, shutdown))]
    pub(crate) async fn apply(
        self,
        database: &Database,
        connection: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        match self {
            Command::Set(cmd) => cmd.apply(database, connection).await,
            Command::Get(cmd) => cmd.apply(database, connection).await,
            Command::Publish(cmd) => cmd.apply(database, connection).await,
            Command::Subscribe(cmd) => cmd.apply(database, connection, shutdown).await,
            Command::Unsubscribe(_) => Err("Unsubscribe is unsupported in this context".into()),
            Command::Ping(cmd) => cmd.apply(connection).await,
            Command::Unknown(cmd) => cmd.apply(connection).await,
            Command::ExitSubscribe(_) => Ok(()),
            Command::Save(cmd) => cmd.apply(database, connection).await,
            Command::Del(cmd) => cmd.apply(database, connection).await,
        }
    }
}
