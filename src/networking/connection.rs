//! Connection结构体，用于从远程peer发送和向远程peer接收Frame

use std::io::Cursor;

use bytes::{Buf, BytesMut};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::networking::frame::{Error::Incomplete, Frame};

/// 用于从远程peer发送和接收Frame，Connection的目的是在底层的TcpStream上读取和写入帧
#[derive(Debug)]
pub struct Connection {
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

    /// # parse_frame() 函数
    ///
    /// 从缓冲区中解析出一个完整的帧
    fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
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

    /// # read_frame() 函数
    ///
    /// - 等到一个完整的帧读取完毕后才返回
    /// - 如果读到多个帧，第一个 帧会被返回，剩下的数据依然被缓冲起来，等待下一次调用
    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
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

    /// # write_frame() 函数
    ///
    /// redis协议编码过程：将一个完整的数据帧写入到socket中
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Array(val) => {
                // 编码帧类型前缀
                self.stream.write_u8(b'*').await?;

                // 编码帧的长度
                self.write_decimal(val.len() as u64).await?;

                // 迭代并编码数组中的每一个元素
                for item in &**val {
                    self.write_value(item).await?;
                }
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

    /// # write_value() 函数
    ///
    /// redis协议编码过程：将字面值帧写入stream
    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => {
                // 编码帧类型前缀
                self.stream.write_u8(b'+').await?;
                // 编码帧的值
                self.stream.write_all(val.as_bytes()).await?;
                // 编码帧的结束符
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(val) => {
                //  编码帧类型前缀
                self.stream.write_u8(b'-').await?;
                // 编码帧的值
                self.stream.write_all(val.as_bytes()).await?;
                // 编码帧的结束符
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                // 编码帧类型前缀
                self.stream.write_u8(b':').await?;
                // 编码帧的值
                self.write_decimal(*val).await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Bulk(val) => {
                let len = val.len();

                // 编码帧类型前缀
                self.stream.write_u8(b'$').await?;
                // 编码Bulk帧的长度
                self.write_decimal(len as u64).await?;
                // 编码Bulk帧的值
                self.stream.write_all(val).await?;
                // 编码帧的结束符
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Array(_val) => unreachable!(),
        }

        Ok(())
    }

    /// # write_decimal() 函数
    ///
    /// redis协议编码过程：将一个十进制帧写入stream
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tokio::{
        io::{self, AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    #[tokio::test]
    async fn test_write_decimal() -> io::Result<()> {
        // 创建一个TcpListener
        let listener = TcpListener::bind("localhost:0").await?;
        let addr = listener.local_addr()?;

        // 创建一个任务来接受连接
        let server = tokio::spawn(async move {
            let (server_stream, _) = listener.accept().await.unwrap();
            let mut connection = Connection::new(server_stream);

            // 服务器写入一个十进制数
            connection.write_decimal(12345).await?;
            // 确保数据被发送到客户端
            connection.stream.flush().await?;

            io::Result::Ok(())
        });

        // 创建一个客户端来连接到服务器
        let mut client = TcpStream::connect(addr).await?;

        // 客户端读取数据
        let mut buf = Vec::new();
        client.read_to_end(&mut buf).await?;

        // 检查写入的数据是否正确
        assert_eq!(buf, b"12345\r\n");

        // 等待服务器任务结束
        server.await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_write_value() -> io::Result<()> {
        // 创建一个TcpListener
        let listener = TcpListener::bind("localhost:0").await?;
        let addr = listener.local_addr()?;

        // 创建一个任务来接受连接
        let server = tokio::spawn(async move {
            let (server_stream, _) = listener.accept().await.unwrap();
            let mut connection = Connection::new(server_stream);

            // 服务器写入不同类型的帧
            connection.write_value(&Frame::Simple("OK".into())).await?;
            connection.write_value(&Frame::Error("ERR".into())).await?;
            connection.write_value(&Frame::Integer(12345)).await?;
            connection.write_value(&Frame::Null).await?;
            connection
                .write_value(&Frame::Bulk(Bytes::from("bulk data")))
                .await?;
            // 确保数据被发送到客户端
            connection.stream.flush().await?;

            io::Result::Ok(())
        });

        // 创建一个客户端来连接服务器
        let mut client = TcpStream::connect(addr).await?;

        // 客户端读取数据
        let mut buf = Vec::new();
        client.read_to_end(&mut buf).await?;

        // 检查写入的数据是否正确
        let expected = b"+OK\r\n-ERR\r\n:12345\r\n$-1\r\n$9\r\nbulk data\r\n";
        assert_eq!(buf, expected);

        // 等待服务器任务结束
        server.await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_read_and_write_frame() -> crate::Result<()> {
        // 创建一个TcpListener
        let listener = TcpListener::bind("localhost:0").await?;
        let addr = listener.local_addr()?;

        // 创建一个任务来接受连接
        let server = tokio::spawn(async move {
            let (mut server_stream, _) = listener.accept().await.unwrap();
            let mut connection = Connection::new(server_stream);

            // 服务器写入不同类型的帧
            connection
                .write_frame(&Frame::Array(vec![
                    Frame::Simple("OK".into()),
                    Frame::Error("ERR".into()),
                    Frame::Integer(12345),
                    Frame::Null,
                    Frame::Bulk(Bytes::from("bulk data")),
                ]))
                .await?;

            crate::Result::Ok(())
        });

        // 创建一个客户端来连接服务器
        let client_stream = TcpStream::connect(addr).await?;
        let mut connection = Connection::new(client_stream);

        // 客户端读取数据并解析帧
        if let Some(frame) = connection.read_frame().await? {
            match frame {
                Frame::Array(val) => {
                    assert_eq!(val.len(), 5);
                    match &val[0] {
                        Frame::Simple(val) => assert_eq!(val, "OK"),
                        _ => panic!("帧类型不是Simple"),
                    }
                    match &val[1] {
                        Frame::Error(val) => assert_eq!(val, "ERR"),
                        _ => panic!("帧类型不是Error"),
                    }
                    match &val[2] {
                        Frame::Integer(val) => assert_eq!(*val, 12345),
                        _ => panic!("帧类型不是Integer"),
                    }
                    match &val[3] {
                        Frame::Null => (),
                        _ => panic!("帧类型不是Null"),
                    }
                    match &val[4] {
                        Frame::Bulk(val) => assert_eq!(val, &Bytes::from("bulk data")),
                        _ => panic!("帧类型不是Bulk"),
                    }
                }
                _ => panic!("帧类型不是Array"),
            }
        } else {
            panic!("没有读取到帧");
        }

        // 等待服务器任务结束
        server.await?;

        Ok(())
    }
}
