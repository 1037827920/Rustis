//! set命令的实现

use bytes::Bytes;

use crate::networking::parse::Parse;

/// # set 命令
///
/// 从key映射到value，如果key已经映射到了一个值，那么旧值将被替换
#[derive(Debug)]
pub struct Set {
    /// 键
    key: String,
    /// 值
    value: Bytes,
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
    pub fn new(key: impl ToString, value: Bytes) -> Self {
        Self {
            key: key.to_string(),
            value,
        }
    }

    /// # key() 函数
    ///
    /// 获取key
    ///
    /// # 返回  
    ///
    /// key
    pub fn key(&self) -> &str {
        &self.key
    }

    /// # value() 函数
    ///
    /// 获取value
    ///
    /// # 返回
    ///
    /// value
    pub fn value(&self) -> &Bytes {
        &self.value
    }

    /// # parse_frames() 函数
    ///
    /// 解析帧
    ///
    /// # 参数
    pub fn parse_frames(parse: &mut Parse) -> crate::Result<Set> {
        // 读取key
        let key = parse.next_string()?;
        // 读取value
        let value = parse.next_bytes()?;

        Ok(Self { key, value })
    }
}
