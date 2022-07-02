use std::env::consts::OS;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use crate::channel::{TcpChannel, TcpSocketChannel};

const DEFAULT_CHARSET_NUMBER: u8 = 33;
const SO_TIMEOUT: u32 = 30 * 1000;
const CONN_TIMEOUT: u32 = 5 * 1000;
const RECEIVE_BUFFER_SIZE: u32 = 16 * 1024;
const SEND_BUFFER_SIZE: u32 = 16 * 1024;
const TIMEOUT: u32 = 5 * 1000;

struct MysqlConnector<'a> {
    address: &'a str,
    port: u16,
    username: &'a str,
    password: &'a str,
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
            address: "",
            port: 0 as u16,
            username: "",
            password: "",
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
    pub fn new_user_pass(address: &'a str, port: u16, username: &'a str, password: &'a str) -> Self {
        Self {
            address,
            port,
            username,
            password,
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
            address,
            port,
            username,
            password,
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


    fn connect(&mut self) {
        let result = self.connected.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst);
        if result.ok().unwrap() {
            self.channel = Option::Some(Box::new(TcpChannel::new(self.address, self.port).or_else(|e| {
                TcpChannel::new(self.address, self.port)
            }).unwrap()));



        } else if result.err().unwrap() {
            println!("the connection is already established")
        }
    }
}
