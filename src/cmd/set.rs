//! set命令的实现

use bytes::Bytes;
use tokio::time::Duration;
use tracing::debug;

use crate::{
    networking::{
        connection::Connection, frame::Frame, parse::{Parse, ParseError}
    },
    persistence::database::Database,
};

/// # set 命令
///
/// 从key映射到value，如果key已经映射到了一个值，那么旧值将被替换
#[derive(Debug)]
pub struct Set {
    /// 键
    key: String,
    /// 值
    value: Bytes,
    /// 过期时间
    expire: Option<Duration>,
}

impl Set {
    /// # new() 函数
    ///
    /// 创建一个新的Set命令
    ///
    /// # 参数
    ///
    /// - `key` 键: 实现了ToString的类型
    /// - `value` 值：Bytes类型
    ///
    /// # 返回
    ///
    /// Set命令
    pub(crate) fn new(key: impl ToString, value: Bytes, expire: Option<Duration>) -> Self {
        Self {
            key: key.to_string(),
            value,
            expire,
        }
    }

    /// # key() 函数
    ///
    /// 获取key
    ///
    /// # 返回  
    ///
    /// key
    #[allow(dead_code)]
    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    /// # value() 函数
    ///
    /// 获取value
    ///
    /// # 返回
    ///
    /// value
    #[allow(dead_code)]
    pub(crate) fn value(&self) -> &Bytes {
        &self.value
    }

    /// # expire() 函数
    ///
    /// 获取过期时间
    ///
    /// # 返回
    ///
    /// 过期时间
    #[allow(dead_code)]
    pub(crate) fn expire(&self) -> Option<Duration> {
        self.expire
    }

    /// # decode_set_from_frame() 函数
    ///
    /// 将帧解码为set命令
    pub(crate) fn decode_set_from_frame(parse: &mut Parse) -> crate::Result<Set> {
        use ParseError::EndOfStream;

        // 读取key
        let key = parse.next_string()?;
        // 读取value
        let value = parse.next_bytes()?;
        // 过期时间
        let mut expire = None;

        // 处理过期时间选项
        match parse.next_string() {
            // 过期时间以秒为单位
            Ok(str) if str.to_lowercase() == "ex" => {
                // 读取过期时间
                let seconds = parse.next_int()?;
                // 设置过期时间
                expire = Some(Duration::from_secs(seconds));
            }
            // 过期时间以毫秒为单位
            Ok(str) if str.to_lowercase() == "px" => {
                // 读取过期时间
                let milliseconds = parse.next_int()?;
                // 设置过期时间
                expire = Some(Duration::from_millis(milliseconds));
            }
            // 未知选项
            Ok(_) => {
                return Err("Currently, the set command only supports EX and PX options".into());
            }
            // 读取结束
            Err(EndOfStream) => {}
            // 其他错误
            Err(err) => return Err(err.into()),
        }

        // 返回Set命令
        Ok(Set::new(key, value, expire))
    }

    /// # code_set_into_frame() 函数
    /// 
    /// 将set命令编码为帧
    pub(crate) fn code_set_into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("set".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame.push_bulk(self.value);

        // 处理过期时间
        if let Some(expire) = self.expire {
            // 选择px，它允许更高的精度
            frame.push_bulk(Bytes::from("px".as_bytes()));
            frame.push_int(expire.as_millis() as u64);
        }

        frame
    }

    /// # apply() 函数
    ///
    /// 应用SET命令
    pub(crate) async fn apply(
        self,
        database: &Database,
        connection: &mut Connection,
    ) -> crate::Result<()> {
        // 往数据库中设置键值对
        database.set(self.key, self.value, self.expire);

        // 创建一个成功的响应
        let response = Frame::Simple("OK".to_string());
        debug!(?response);

        // 往流中写入响应
        connection.write_frame(&response).await?;

        Ok(())
    }
}
