//! 解析数据帧

use bytes::Bytes;
use core::fmt;
/// # Parse 结构体
///
/// 用于解析数据帧
use std::{str, vec, fmt};

use super::frame::Frame;

type Result<T> = std::result::Result<T, ParseError>;

#[derive(Debug)]
pub(crate) struct Parse {
    /// 数组帧迭代器
    arr_frame_iter: vec::IntoIter<Frame>,
}

impl Parse {
    /// # new() 函数
    ///
    /// 创建一个新的Parse，用于解析数据帧
    ///
    /// # 返回
    ///
    /// 返回ParseError当帧不是数组帧
    pub(crate) fn new(frame: Frame) -> Result<Parse> {
        let array = match frame {
            Frame::Array(array) => array,
            frame => return Err(format!("协议错误: 期望是array帧, 但是获得了{:?}", frame).into()),
        };

        Ok(Parse {
            arr_frame_iter: array.into_iter(),
        })
    }

    /// # next() 函数
    ///
    /// 返回下一项
    fn next(&mut self) -> Result<Frame> {
        self.arr_frame_iter.next().ok_or(ParseError::EndOfStream)
    }

    /// # next_string() 函数
    ///
    /// 将下一项以String类型返回
    ///
    /// # 返回
    ///
    /// 如果下一项无法转换为String，则返回PraseError
    pub(crate) fn next_string(&mut self) -> Result<String> {
        match self.next()? {
            Frame::Simple(s) => Ok(s),
            Frame::Bulk(data) => str::from_utf8(&data[..])
                .map(|s| s.to_string())
                .map_err(|_| "协议错误: 无效的字符串".into()),
            frame => Err(format!("协议错误: 期望是Simple或Bulk帧, 但是获得了{:?}", frame).into()),
        }
    }

    /// # next_bytes() 函数
    ///
    /// 将下一项以Bytes类型返回
    pub(crate) fn next_bytes(&mut self) -> Result<Bytes> {
        match self.next()? {
            Frame::Simple(s) => Ok(Bytes::from(s.into_bytes())),
            Frame::Bulk(data) => Ok(data),
            frame => Err(format!("协议错误: 期望是Simple或Bulk帧, 但是获得了{:?}", frame).into()),
        }
    }

    /// # next_int() 函数
    ///
    /// 将下一项以u64类型返回
    pub(crate) fn next_int(&mut self) -> Result<u64> {
        use atoi::atoi;

        const ERR_MSG: &str = "协议错误: 无效的整数";

        match self.next()? {
            Frame::Integer(value) => Ok(value),
            Frame::Simple(str) => atoi::<u64>(str.as_bytes()).ok_or_else(|| ERR_MSG.into()),
            Frame::Bulk(data) => atoi::<u64>(&data).ok_or_else(|| ERR_MSG.into()),
            frame => Err(format!(
                "协议错误: 期望是Integer, Simple或Bulk帧, 但是获得了{:?}",
                frame
            )
            .into()),
        }
    }

    /// # is_empty() 函数
    ///
    /// 确保没有剩余的帧
    pub(crate) fn is_finish(&mut self) -> Result<()> {
        if self.arr_frame_iter.next().is_none() {
            Ok(())
        } else {
            Err("协议错误: 期望是没有帧了, 但数组帧包含多余的帧".into())
        }
    }
}

/// # PraseError 枚举
///
/// 当解析错误时的错误类型
#[derive(Debug)]
pub(crate) enum ParseError {
    /// 由于帧已满，试图提取值失败
    EndOfStream,
    /// 其他错误
    Other(crate::Error),
}

impl From<String> for ParseError {
    fn from(err: String) -> ParseError {
        ParseError::Other(err.into())
    }
}

impl From<&str> for ParseError {
    fn from(err: &str) -> ParseError {
        err.to_string().into()
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::EndOfStream => "协议错误: 已经不能读取更多的数据帧".fmt(f),
            ParseError::Other(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for ParseError {}