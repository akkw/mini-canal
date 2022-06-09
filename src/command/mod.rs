use std::borrow::BorrowMut;
use std::io::{Error, ErrorKind, Read, Write, Result};
use std::net::{SocketAddrV4, TcpStream};


trait SocketChannel {
    fn write(&mut self, buf: &[u8]) -> Result<usize>;
    fn read(&mut self, buf: &mut [u8]) -> Result<usize>;
    fn read_with_timeout(&mut self, buf: &mut [u8], timeout: i32) -> Result<usize>;

    fn is_connected(&self) -> &bool;
    fn get_remote_address(&self) -> Option<SocketAddrV4>;
    fn get_local_address(&self) -> Option<SocketAddrV4>;
    fn close(&self) -> std::result::Result<&bool, Error>;
}


struct TcpChannel {
    channel: TcpStream,
    address: Option<SocketAddrV4>,
}

// 默认超时时间
const DEFAULT_CONNECT_TIMEOUT: i32 = 10 * 1000;
//
const SO_TIMEOUT: i32 = 1000;

impl SocketChannel for TcpChannel {


    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let size = self.channel.write(buf)?;
        Ok(size)
    }

    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.channel.read(buf)
    }

    fn read_with_timeout(&mut self, buf: &mut [u8], timeout: i32) -> Result<usize> {
        let mut tmp = [0u8; 1];
        let len = buf.len();
        self.channel.read(&mut tmp)
    }

    fn is_connected(&self) -> &bool {
        todo!()
    }

    fn get_remote_address(&self) -> Option<SocketAddrV4> {
        todo!()
    }

    fn get_local_address(&self) -> Option<SocketAddrV4> {
        todo!()
    }

    fn close(&self) -> std::result::Result<&bool, Error> {
        todo!()
    }
}
