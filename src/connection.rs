use std::io::Cursor;

use bytes::{Buf, BytesMut};
use mini_redis::{frame::Error::Incomplete, Frame, Result};
use tokio::{io::{self, AsyncReadExt, AsyncWriteExt, BufWriter}, net::TcpStream};

/// 用于从远程对等待方发送和接收Frame，Connection的目的是在底层的TcpStream上读取和写入帧
struct Connection {
    /// BufWriter，当write方法被调用时，不会直接写入到socket，而是先写入到缓冲区中，
    /// 当缓冲区填满时，会自动刷新到内部的socket中，然后再将缓冲区清空
    /// 这样做的目的是为了减少系统调用的次数，提高性能
    stream: BufWriter<TcpStream>,
    /// 为了读取帧，Connection使用一个内部缓冲区，该缓冲区会被填充，直到有足够的字节来创建一个完整的帧，一旦缓冲区
    /// 中有足够的数据，Connection就会创建帧并将其返回给调用者
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(stream),
            // 分配一个缓冲区，具有4KB的缓冲长度
            buffer: BytesMut::with_capacity(4096),
        }
    }

    /// # 函数功能
    ///
    /// - 等到一个完整的帧读取完毕后才返回
    /// - 如果读到多个帧，第一个 帧会被返回，剩下的数据依然被缓冲起来，等待下一次调用
    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            // 尝试从缓冲区的数据中解析出一个数据帧
            if let Some(frame) = self.parse_frame()? {
                // 只有当数据足够被解析时，才返回对应的帧
                return Ok(Some(frame));
            }

            // 如果缓冲区中的数据还不足以被解析为一个数据帧
            // 就需要从socket中读取更多的数据
            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                // 读取成功时，会返回读取到的字节数，0代表读到了stream的末尾
                // 这时候对端已经关闭了连接

                // 检查缓冲区是否为空
                if self.buffer.is_empty() {
                    // 没有数据，说明数据成功被处理
                    return Ok(None);
                } else {
                    // 缓冲区中还有数据，说明对端关闭了连接，但是还有数据没有被处理
                    return Err("连接被对端重置".into());
                }
            }
        }
    }

    /// # 函数功能
    /// 从缓冲区中解析出一个完整的帧
    fn parse_frame(&mut self) -> Result<Option<Frame>> {
        // 创建T: Buf类型
        let mut buf = Cursor::new(&self.buffer[..]);

        // 检查是否读取了一个足够解析出一个帧的数据
        match Frame::check(&mut buf) {
            Ok(_) => {
                // 获取组成该帧的字节数
                let len = buf.position() as usize;

                // 在解析开始前，重置内部的游标位置
                buf.set_position(0);

                // 解析帧
                let frame = Frame::parse(&mut buf)?;

                // 解析完成，从缓冲去中移除已经解析的数据
                self.buffer.advance(len); // advance()用于前进buf的内部游标，从而丢弃这些字节

                // 返回解析出的帧
                Ok(Some(frame))
            }
            // 缓冲区的数据不足以解析出一个完整的帧
            Err(Incomplete) => Ok(None),
            // 解析出错
            Err(e) => Err(e.into()),
        }
    }

    /// # 函数功能
    /// 将一个完整的帧写入socket
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Array(val) => {
                unimplemented!();
            }
            // 帧类型为字面量，直接对值进行编码并写入到socket
            _ => self.write_value(frame).await?,
        }

        // 调用flush()将缓冲区剩余的内容写入socket
        // TODO: 当帧比较小的时候，每写一次帧就flush一次的模式性能开销比较大
        // 此时我们可以选择在Connection中实现flush函数，然后等帧积累多个后，
        // 再一次性在Connection中进行flush
        self.stream.flush().await
    }

    /// # 函数功能
    /// 将帧字面值写入stream
    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Bulk(val ) => {
                let len = val.len();

                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Array(_val) => unreachable!(),
        }

        Ok(())
    }

    /// # 函数功能
    /// 将一个十进制帧写入stream
    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;

        // 创建一个20字节的缓冲区
        let mut buf = [0u8; 20];
        // 创建一个Cursor，用于在buf上进行读写操作
        let mut buf = Cursor::new(&mut buf[..]);
        // 将val格式化为字符串并写入到buf中
        write!(&mut buf, "{}", val)?;

        // 获取buf的游标位置，即有数据的最后一个位置
        let pos = buf.position() as usize;
        // 将buf中的数据写入到stream中
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        // 写入换行符
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}
