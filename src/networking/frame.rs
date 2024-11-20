//! 客户端和服务器之间通信的数据单元，符合RESP协议

use bytes::{Buf, Bytes};
use std::{
    fmt::{self, format},
    io::{Cursor, Read},
    num::TryFromIntError,
    string::FromUtf8Error,
};

#[derive(Debug)]
pub enum Frame {
    /// 简单字符串
    Simple(String),
    /// 错误
    Error(String),
    /// 整数
    Integer(u64),
    /// Bulk
    Bulk(Bytes),
    /// null
    Null,
    /// 数组帧
    Array(Vec<Frame>),
}

impl Frame {
    /// # array() 函数
    ///
    /// 返回一个空数组帧
    pub(crate) fn array() -> Frame {
        Frame::Array(Vec::new())
    }

    /// # push_bulk() 函数
    ///
    /// 将一个bulk帧推入数组，self必须是一个数组帧
    ///
    /// # panic
    ///
    /// 如果self不是一个数组帧，将会panic
    pub(crate) fn push_bulk(&mut self, data: Bytes) {
        match self {
            Frame::Array(vec) => vec.push(Frame::Bulk(data)),
            _ => panic!("插入bulk帧时, 被插入的帧类型不是数组帧"),
        }
    }

    /// # push_int() 函数
    ///
    /// 将一个整数帧推入数组，self必须是一个数组帧
    ///
    /// # panic
    ///
    /// 如果self不是一个数组帧，将会panic
    pub(crate) fn push_int(&mut self, value: u64) {
        match self {
            Frame::Array(vec) => vec.push(Frame::Integer(value)),
            _ => panic!("插入整数帧时, 被插入的帧类型不是数组帧"),
        }
    }

    /// # check() 函数
    ///
    /// 检查是否可以从src解码整个消息
    pub(crate) fn check(src: &mut Cursor<&[u8]>) -> Result<()> {
        match get_u8(src)? {
            b'+' => {
                // Simple(String)
                get_line(src)?;
                Ok(())
            }
            b'-' => {
                // Error(String)
                get_line(src)?;
                Ok(())
            }
            b':' => {
                // Integer(u64)
                let _ = get_decimal(src)?;
                Ok(())
            }
            b'$' => {
                // Bulk(Bytes)
                if b'-' == peek_u8(src)? {
                    // 跳过 $<数据长度>\r\n
                    skip(src, 4)
                } else {
                    // 获取Bulk的长度
                    let len: usize = get_decimal(src)?.try_into()?;

                    // 跳过前面的标识字符+长度（2个字节
                    skip(src, len + 2)
                }
            }
            b'*' => {
                // Array(Vec<Frame>)
                // 获取数组的长度
                let len = get_decimal(src)?;

                for _ in 0..len {
                    Frame::check(src)?;
                }

                Ok(())
            }
            actual => Err(format!("协议错误: 无效的帧类型: {}", actual).into()),
        }
    }

    /// # parse() 函数
    ///
    /// redis协议解码过程：从src中解析一个Frame，要先调用check()函数检查是否可以解析
    pub(crate) fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame> {
        match get_u8(src)? {
            b'+' => {
                // Simple(String)
                // 读取行数据，并将其转换为Vec<u8>
                let line = get_line(src)?.to_vec();

                // 将Vec<u8>转换为String
                let string = String::from_utf8(line)?;

                Ok(Frame::Simple(string))
            }
            b'-' => {
                // Error(String)
                // 读取行数据，并将其转换为Vec<u8>
                let line = get_line(src)?.to_vec();

                // 将Vec<u8>转换为String
                let string = String::from_utf8(line)?;

                Ok(Frame::Error(string))
            }
            b':' => {
                // Integer(u64)
                // 获取十进制数
                let value = get_decimal(src)?;
                Ok(Frame::Integer(value))
            }
            b'$' => {
                // Buik(Bytes)
                if b'-' == peek_u8(src)? {
                    let line = get_line(src)?;

                    if line != b"-1" {
                        return Err("协议错误: 无效的帧格式".into());
                    }

                    Ok(Frame::Null)
                } else {
                    // 读取bulk的长度
                    let len: usize = get_decimal(src)?.try_into()?;
                    let n = len + 2;

                    // 检查是否有足够的数据组成一个帧
                    if src.remaining() < n {
                        return Err(Error::Incomplete);
                    }

                    let data = Bytes::copy_from_slice(&src.chunk()[..len]);

                    // 跳过数据
                    skip(src, n)?;

                    Ok(Frame::Bulk(data))
                }
            }
            b'*' => {
                // Array(Vec<Frame>)
                // 获取数组的长度
                let len = get_decimal(src)?.try_into()?;
                let mut out = Vec::with_capacity(len);

                for _ in 0..len {
                    out.push(Frame::parse(src)?);
                }

                Ok(Frame::Array(out))
            }
            _ => unimplemented!(),
        }
    }

    /// # to_error() 函数
    ///
    /// 将Frame转换为Error类型
    pub(crate) fn to_error(&self) -> crate::Error {
        format!("不是预期的帧类型: {:?}", self).into()
    }
}

/// get_u8() 函数
///
/// 从Cursor中获取一个u8类型的字节
fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u8())
}

/// get_line() 函数
///
/// 从Cursor中获取一行数据
fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8]> {
    let start = src.position() as usize;
    let end = src.get_ref().len() - 1;

    for i in start..end {
        // 如果找到了换行符
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            // 将游标移动到下一行的开头
            src.set_position((i + 2) as u64);

            // 返回找到的行
            return Ok(&src.get_ref()[start..i]);
        }
    }

    Err(Error::Incomplete)
}

/// # get_decimal() 函数
///
/// 从Cursor中读取一个以新行结尾的十进制数
fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64> {
    use atoi::atoi;

    let line = get_line(src)?;

    atoi::<u64>(line).ok_or_else(|| "协议错误: 无效的帧格式".into())
}

/// # peek_u8() 函数
///
/// 从Cursor中查看一个u8类型的字节(不消费)
fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    // chunk()返回当前的字节切片，[0]表示取第一个字节
    Ok(src.chunk()[0])
}

/// # skip() 函数
///
/// 从Cursor中跳过n个字节
fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<()> {
    if src.remaining() < n {
        return Err(Error::Incomplete);
    }

    src.advance(n);
    Ok(())
}

#[derive(Debug)]
pub(crate) enum Error {
    /// 没有足够的数组去解析一个完整的帧
    Incomplete,
    /// 无效的帧
    Other(crate::Error),
}

type Result<T> = std::result::Result<T, Error>;

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Incomplete => "数据不够组成一个完整的帧".fmt(f),
            Error::Other(err) => err.fmt(f),
        }
    }
}

impl From<String> for Error {
    fn from(err: String) -> Error {
        Error::Other(err.into())
    }
}

impl From<&str> for Error {
    fn from(err: &str) -> Error {
        err.to_string().into()
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_err: FromUtf8Error) -> Error {
        "协议错误: 无效的帧格式".into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_err: TryFromIntError) -> Error {
        "协议错误: 无效的帧格式".into()
    }
}
