use std::sync::atomic::{AtomicBool, Ordering};
use std::str::from_utf8;
use crate::channel::{TcpChannel, TcpSocketChannel};
use crate::channel::read_write_packet::{*};
use crate::command::{msc, Packet, write_header_and_body};
use crate::command::server::{*};
use crate::utils::mysql_password_encrypted::{*};
use crate::command::client::{*};


const DEFAULT_CHARSET_NUMBER: u8 = 33;
const SO_TIMEOUT: u32 = 30 * 1000;
const CONN_TIMEOUT: u32 = 5 * 1000;
const RECEIVE_BUFFER_SIZE: u32 = 16 * 1024;
const SEND_BUFFER_SIZE: u32 = 16 * 1024;
const TIMEOUT: u32 = 5 * 1000;

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
    connection_id: u32,
    connected: AtomicBool,
    server_version: String,
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
            connection_id: 0,
            connected: AtomicBool::new(false),
            server_version: String::new(),
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
            connection_id: 0,
            connected: AtomicBool::new(false),
            server_version: String::new(),
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
            connection_id: 0,
            connected: AtomicBool::new(false),
            server_version: String::new(),
            timeout: TIMEOUT, // 5s
        }
    }


    pub fn connect(&mut self) {
        let result = self.connected.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed);
        if result.is_ok() {
            self.channel = Option::Some(Box::new(TcpChannel::new(self.address.unwrap(), self.port).or_else(|_e| {
                TcpChannel::new(self.address.unwrap(), self.port)
            }).unwrap()));

            self.negotiate();
        } else if result.is_err() {
            println!("the connection is already established")
        }
    }

    fn negotiate<'b: 'a>(&mut self) -> Result<bool, String> {
        let header = read_header_timeout(self.channel.as_mut().unwrap(), TIMEOUT).unwrap();
        let body = read_bytes(self.channel.as_mut().unwrap(), header.packet_body_length());


        if body[0] < 0 {
            return if body[0] == 255 {
                let mut error = ErrorPacket::new();
                error.from_bytes(&body);
                Result::Err(format!("handshake exception: {}", error))
            } else if body[0] == 254 {
                Result::Err(String::from("Unexpected EOF packet at handshake phase."))
            } else {
                Result::Err(format!("Unexpected packet with field_count: {}", body[0]))
            };
        }
        let mut handshake_initialization = HandshakeInitializationPacket::new();
        handshake_initialization.from_bytes(&body);
        if handshake_initialization.protocol_version() != msc::DEFAULT_PROTOCOL_VERSION {
            self.auth323(header.packet_sequence_number(), handshake_initialization.seed());
            return Result::Ok(true);
        }


        self.connection_id = handshake_initialization.thread_id();
        self.server_version = String::from(handshake_initialization.server_version());
        println!("handshake initialization packet received, prepare the client authentication packet to send");

        let mut client_authentication_packet = ClientAuthenticationPacket::new();
        client_authentication_packet.set_charset_number(self.charset_number);
        client_authentication_packet.set_username(self.username.unwrap());
        client_authentication_packet.set_password(self.password.unwrap());
        client_authentication_packet.set_server_capabilities(handshake_initialization.server_capabilities());
        client_authentication_packet.set_database_name("performance_schema");
        let scrumble_buff;

        scrumble_buff = self.join_and_create_scrumble_buff(&handshake_initialization);
        client_authentication_packet.set_scrumble_buff(&scrumble_buff);
        client_authentication_packet.set_auth_plugin_name("mysql_native_password".as_bytes());

        let client_authentication_pkg_body = client_authentication_packet.to_bytes();

        let mut h = HeaderPacket::new();
        h.set_packet_body_length(client_authentication_pkg_body.len());
        h.set_packet_sequence_number(header.packet_sequence_number() + 1);
        let client_authentication_pkg_bod = write_header_and_body(&h.to_bytes(), &client_authentication_pkg_body);

        write_pkg(self.channel.as_mut().unwrap(), &client_authentication_pkg_bod);

        println!("client authentication packet is sent out.");

        let header = read_header(self.channel.as_mut().unwrap());

        let body = self.channel.as_mut().unwrap().read_len(header.unwrap().packet_body_length());

        let marker = body[0];

        if marker == 254 || marker == 1 {
            let mut auth_data;
            let mut plugin_name = "";
            if marker == 1 {
                let mut auth_switch_request_more_data = AuthSwitchRequestMoreData::new();
                auth_switch_request_more_data.from_bytes(&body);
                auth_data = auth_switch_request_more_data.auth_data();
            } else {
                let mut auth_switch_request_packet = AuthSwitchRequestPacket::new();
                auth_switch_request_packet.from_bytes(&body);
                auth_data = auth_switch_request_packet.auth_data();
                plugin_name = auth_switch_request_packet.auth_name();
            }
            let mut is_sha2_password = false;
            let encrypted_password;
            if plugin_name.len() != 0  {
                if "mysql_native_password".eq(plugin_name) {
                    encrypted_password = scramble411(self.password.unwrap().as_bytes(), auth_data);
                } else {
                    is_sha2_password = true;
                    encrypted_password = scrambleCachingSha2(self.password.unwrap().as_bytes(), auth_data);
                }
            }
        }


        if marker == 255 {
            let mut error = ErrorPacket::new();
            error.from_bytes(&body);
            return Result::Err(format!("Error When doing Client Authentication: {}" , error));
        }
        Result::Ok(true)
    }

    fn join_and_create_scrumble_buff(&self, handshakePacket: &HandshakeInitializationPacket) -> Box<[u8]> {
        let mut out = vec![];
        for i in 0..handshakePacket.seed().len() {
            out.push(handshakePacket.seed()[i])
        }
        for i in 0..handshakePacket.rest_of_scramble_buff().len() {
            out.push(handshakePacket.rest_of_scramble_buff()[i])
        }
        Box::from(out)
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
        let pkg = write_header_and_body(&header_h323.to_bytes(), &b323_bdoy);
        write_pkg(self.channel.as_mut().unwrap(), &pkg)
    }
}



