
use std::env::consts::OS;
use std::fmt::format;
use std::io::{Error, ErrorKind};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::str::from_utf8;
use crate::channel::{TcpChannel, TcpSocketChannel};
use crate::channel::read_write_packet::{*};
use crate::command::{msc, Packet};
use crate::command::server::{*};
use crate::utils::mysql_password_encrypted::{*};





const DEFAULT_CHARSET_NUMBER: u8 = 33;
const SO_TIMEOUT: u32 = 30 * 1000;
const CONN_TIMEOUT: u32 = 5 * 1000;
const RECEIVE_BUFFER_SIZE: u32 = 16 * 1024;
const SEND_BUFFER_SIZE: u32 = 16 * 1024;
const TIMEOUT: u32 = 10 * 1000;

pub struct MysqlConnector<'a> {
    address: Option<&'a str>,
    port: u16,
    username: Option<&'a str>,
    password: Option<&'a str>,
    charset_number: u8,
    default_schema: &'a str,
    so_timeout: u32,
    conn_timeout: u32,
    receive_buffer_size: u32,
    send_buffer_size: u32,
    channel: Option<Box<dyn TcpSocketChannel>>,
    dumping: bool,
    connection_id: i64,
    connected: AtomicBool,
    server_version: &'a str,
    timeout: u32,
}

impl<'a> MysqlConnector<'a> {
    pub fn new() -> Self {
        Self {
            address: Option::None,
            port: 0 as u16,
            username: Option::None,
            password: Option::None,
            charset_number: DEFAULT_CHARSET_NUMBER,
            default_schema: "",
            so_timeout: SO_TIMEOUT,
            conn_timeout: CONN_TIMEOUT,
            receive_buffer_size: RECEIVE_BUFFER_SIZE,
            send_buffer_size: SEND_BUFFER_SIZE,
            channel: Option::None,
            dumping: false,
            connection_id: -1,
            connected: AtomicBool::new(false),
            server_version: "",
            timeout: TIMEOUT, // 5s
        }
    }
    pub fn new_user_password(address: &'a str, port: u16, username: &'a str, password: &'a str) -> Self {
        Self {
            address: Option::Some(address),
            port,
            username: Option::Some(username),
            password: Option::Some(password),
            charset_number: DEFAULT_CHARSET_NUMBER,
            default_schema: "",
            so_timeout: SO_TIMEOUT,
            conn_timeout: CONN_TIMEOUT,
            receive_buffer_size: RECEIVE_BUFFER_SIZE,
            send_buffer_size: SEND_BUFFER_SIZE,
            channel: Option::None,
            dumping: false,
            connection_id: -1,
            connected: AtomicBool::new(false),
            server_version: "",
            timeout: TIMEOUT, // 5s
        }
    }

    pub fn new_schema(address: &'a str, port: u16, username: &'a str, password: &'a str, charset_number: u8, default_schema: &'a str) -> Self {
        Self {
            address: Option::Some(address),
            port,
            username: Option::Some(username),
            password: Option::Some(password),
            charset_number,
            default_schema,
            so_timeout: SO_TIMEOUT,
            conn_timeout: CONN_TIMEOUT,
            receive_buffer_size: RECEIVE_BUFFER_SIZE,
            send_buffer_size: SEND_BUFFER_SIZE,
            channel: Option::None,
            dumping: false,
            connection_id: -1,
            connected: AtomicBool::new(false),
            server_version: "",
            timeout: TIMEOUT, // 5s
        }
    }


    pub fn connect(&mut self) {
        let result = self.connected.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed);
        if result.is_ok() {
            self.channel = Option::Some(Box::new(TcpChannel::new(self.address.unwrap(), self.port).or_else(|e| {
                TcpChannel::new(self.address.unwrap(), self.port)
            }).unwrap()));

            self.negotiate();
        } else if result.is_err()  {
            println!("the connection is already established")
        }
    }

    fn negotiate(&mut self) -> Result<bool, String> {
        let mut channel = self.channel.as_mut().unwrap();
        let header = read_header_timeout(channel, TIMEOUT).unwrap();
        let body = read_bytes(channel, header.packet_body_length());
        if body[0] < 0 {
            return if body[0] == 255 {
                let mut error = ErrorPacket::new();
                error.from_bytes(&body);
                Result::Err(format!("handshake exception: {}", error))
            } else if body[0] == 254 {
                Result::Err(String::from("Unexpected EOF packet at handshake phase."))
            } else {
                Result::Err(format!("Unexpected packet with field_count: {}", body[0]))
            }
        }
        let mut handshake_initialization = HandshakeInitializationPacket::new();
        handshake_initialization.from_bytes(&body);
        if handshake_initialization.protocol_version() != msc::DEFAULT_PROTOCOL_VERSION {
            self.auth323(header.packet_sequence_number(), handshake_initialization.seed());
            return Result::Ok(true)
        }
        Result::Ok(true)
    }


    fn auth323(&mut self, packet_sequence_number: u8, seed: &[u8]) {
        let mut r323 = Reply323Packet::new();
        let x;
        if self.password != Option::None && self.password.unwrap().len() > 0 {
            x = scramble323(self.password, Option::Some(from_utf8(&seed).unwrap()));
            r323.set_seed(x.as_bytes());
        }
        let b323_bdoy = r323.to_bytes();
        let mut header_h323 = HeaderPacket::new();
        header_h323.set_packet_body_length(b323_bdoy.len());
        header_h323.set_packet_sequence_number(packet_sequence_number + 1);
        let mut out_packet = vec![];
        out_packet.push(header_h323.to_bytes());
        out_packet.push(b323_bdoy);
        write_pkg(self.channel.as_mut().unwrap(),out_packet.last().unwrap() )
    }
}

