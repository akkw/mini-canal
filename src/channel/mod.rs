use std::io::{Error, ErrorKind, Read, Result, Write};
use std::net::{Ipv4Addr, Shutdown, SocketAddrV4, TcpStream};
use std::str::FromStr;
use chrono::Local;


pub trait TcpSocketChannel: {
    fn write(&mut self, buf: &[u8]) -> Result<usize>;
    fn read(&mut self, buf: &mut [u8]) -> Result<usize>;
    fn read_with_timeout(&mut self, buf: &mut [u8], timeout: u32) -> Result<usize>;
    fn read_len(&mut self, len: i64) -> Box<[u8]>;
    fn read_offset_len(&mut self, buf: &mut [u8], off: usize, len: usize) -> Result<usize>;
    fn is_connected(&self) -> bool;
    fn get_remote_address(&self) -> Option<SocketAddrV4>;
    fn get_local_address(&self) -> Option<SocketAddrV4>;
    fn close(&self) -> Result<()>;
}

pub struct TcpChannel {
    channel: TcpStream,
    address: Option<SocketAddrV4>,
    is_connected: bool,
}

// 默认超时时间
const DEFAULT_CONNECT_TIMEOUT: i32 = 10 * 1000;
//
const SO_TIMEOUT: i32 = 1000;


impl TcpChannel {
    pub fn new(addr: &str, port: u16) -> Result<TcpChannel> {
        TcpStream::connect(format!("{}:{}", addr, port)).map(|channel| {
            let addr = Ipv4Addr::from_str(addr).map(|addr| {
                SocketAddrV4::new(addr, port)
            }).unwrap();
            TcpChannel {
                channel,
                address: Option::Some(addr),
                is_connected: true,
            }
        })
    }
}

impl TcpSocketChannel for TcpChannel {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let size = self.channel.write(buf)?;
        Ok(size)
    }

    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.channel.read(buf)
    }

    fn read_with_timeout(&mut self, buf: &mut [u8], timeout: u32) -> Result<usize> {
        let now = Local::now().timestamp_millis();
        let mut remain = buf.len();
        loop {
            let mut tmp = [0u8; 1];
            let size = self.channel.read(&mut tmp)?;
            buf[buf.len() - remain] = tmp[0];
            remain -= size;
            if remain as i64 <= 0 {
                break;
            }

            if Local::now().timestamp_millis() - now > timeout as i64 {
                return std::result::Result::Err(Error::from(ErrorKind::TimedOut));
            }
        }
        std::result::Result::Ok(buf.len() - remain)
    }

    fn read_len(&mut self, mut len: i64) -> Box<[u8]> {
        let mut out = vec![];
        loop {
            let mut tmp = [0u8; 1];
            let size = self.channel.read(&mut tmp).unwrap() as i64;
            out.push(tmp[0]);
            len -= size;
            if len as i64 <= 0 {
                break;
            }
        }
        Box::from(out)
    }

    fn read_offset_len(&mut self, buf: &mut [u8], off: usize, len: usize) -> Result<usize> {
        self.channel.read(&mut buf[off..off + len])
    }

    fn is_connected(&self) -> bool {
        self.is_connected
    }

    fn get_remote_address(&self) -> Option<SocketAddrV4> {
        todo!()
    }

    fn get_local_address(&self) -> Option<SocketAddrV4> {
        let addr = self.channel.local_addr().ok();
        match addr {
            Some(addr) => {
                let ip = addr.ip();
                let ip_byte = ip.to_string();
                let ip_byte = ip_byte.as_bytes();
                Option::from(SocketAddrV4::new(Ipv4Addr::new(ip_byte[0], ip_byte[1], ip_byte[2], ip_byte[3]), addr.port()))
            }
            None => {
                Option::None
            }
        }
    }

    fn close(&self) -> Result<()> {
        return self.channel.shutdown(Shutdown::Both);
    }
}


pub mod mysql_socket;
pub mod read_write_packet;
pub mod sql_utils;
