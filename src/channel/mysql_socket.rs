use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::str::{Chars, from_utf8};
use crate::channel::{TcpChannel, TcpSocketChannel};
use crate::channel::read_write_packet::{*};
use crate::command::{log_event, msc, Packet, write_header_and_body};
use crate::command::server::{*};
use crate::utils::mysql_password_encrypted::{*};
use crate::command::client::{*};
use crate::parse::inbound::{MultiStageCoprocessor, SinkFunction};
use crate::parse::support::AuthenticationInfo;


const DEFAULT_CHARSET_NUMBER: u8 = 33;
const SO_TIMEOUT: u32 = 30 * 1000;
const CONN_TIMEOUT: u32 = 5 * 1000;
const RECEIVE_BUFFER_SIZE: u32 = 16 * 1024;
const SEND_BUFFER_SIZE: u32 = 16 * 1024;
const TIMEOUT: u32 = 5 * 1000;

pub struct MysqlConnector {
    address: String,
    port: u16,
    username: String,
    password: Option<String>,
    charset_number: u8,
    default_schema: String,
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

impl MysqlConnector {
    pub fn new() -> Self {
        Self {
            address: String::new(),
            port: 0 as u16,
            username: String::new(),
            password: Option::Some(String::new()),
            charset_number: DEFAULT_CHARSET_NUMBER,
            default_schema: String::new(),
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
    pub fn new_user_password(address: String, port: u16, username: String, password: String) -> Self {
        Self {
            address,
            port,
            username,
            password: Option::Some(password),
            charset_number: DEFAULT_CHARSET_NUMBER,
            default_schema: String::new(),
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

    pub fn new_schema(address: String, port: u16, username: String, password: String, charset_number: u8, default_schema: String) -> Self {
        Self {
            address,
            port,
            username,
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
            self.channel = Option::Some(Box::new(TcpChannel::new(&self.address, self.port).or_else(|_e| {
                TcpChannel::new(&self.address, self.port)
            }).unwrap()));

            self.negotiate();
        } else if result.is_err() {
            println!("the connection is already established")
        }
    }

    fn negotiate(&mut self) -> Result<bool, String> {
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
        client_authentication_packet.set_username(&self.username);
        client_authentication_packet.set_password(self.password.as_ref().unwrap());
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

        let header = read_header(self.channel.as_mut().unwrap()).unwrap();

        let body = self.channel.as_mut().unwrap().read_len(header.packet_body_length());

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


            if plugin_name.len() != 0 && "mysql_native_password".eq(plugin_name) {
                encrypted_password = scramble411(self.password.as_ref().unwrap().as_bytes(), auth_data);
            } else if plugin_name.len() != 0 && "caching_sha2_password".eq(plugin_name) {
                is_sha2_password = true;
                encrypted_password = scrambleCachingSha2(self.password.as_ref().unwrap().as_bytes(), auth_data);
            } else {
                encrypted_password = Box::from([]);
            }

            let i = (&encrypted_password).len();
            assert_ne!(i, 0);
            let mut auth_switch_response_packet = AuthSwitchResponsePacket::new();
            auth_switch_response_packet.set_auth_data(&encrypted_password);

            let mut h = HeaderPacket::new();
            h.set_packet_body_length(encrypted_password.len());
            h.set_packet_sequence_number(header.packet_sequence_number());
            let auth_switch_response_packet_bytes = write_header_and_body(&h.to_bytes(), &auth_switch_response_packet.to_bytes());
            write_pkg(self.channel.as_mut().unwrap(), &auth_switch_response_packet_bytes);

            println!("auth switch response packet is sent out.");

            let mut header = read_header(self.channel.as_mut().unwrap()).unwrap();
            let body = read_bytes(self.channel.as_mut().unwrap(), header.packet_body_length());
            assert_ne!(body.len(), 0);

            if is_sha2_password {
                if body[0] == 0x01 && body[1] == 0x04 {
                    return Result::Err(format!("Error When doing Client Authentication"));
                }
                header = read_header(self.channel.as_mut().unwrap()).unwrap();
                read_bytes(self.channel.as_mut().unwrap(), header.packet_body_length());
            }
        }


        if marker == 255 {
            let mut error = ErrorPacket::new();
            error.from_bytes(&body);
            return Result::Err(format!("Error When doing Client Authentication: {}", error));
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
        if self.password != Option::None && self.password.as_ref().unwrap().len() > 0 {
            x = scramble323(Option::Some(&self.password.as_ref().unwrap()), Option::Some(from_utf8(&seed).unwrap()));
            r323.set_seed(x.as_bytes());
        }
        let b323_bdoy = r323.to_bytes();
        let mut header_h323 = HeaderPacket::new();
        header_h323.set_packet_body_length(b323_bdoy.len());
        header_h323.set_packet_sequence_number(packet_sequence_number + 1);
        let pkg = write_header_and_body(&header_h323.to_bytes(), &b323_bdoy);
        write_pkg(self.channel.as_mut().unwrap(), &pkg)
    }
    pub fn set_address(&mut self, address: String) {
        self.address = address;
    }
    pub fn set_port(&mut self, port: u16) {
        self.port = port;
    }
    pub fn set_username(&mut self, username: String) {
        self.username = username;
    }
    pub fn set_password(&mut self, password: String) {
        self.password = Option::Some(password);
    }
    pub fn set_charset_number(&mut self, charset_number: u8) {
        self.charset_number = charset_number;
    }
    pub fn set_default_schema(&mut self, default_schema: String) {
        self.default_schema = default_schema;
    }
    pub fn set_so_timeout(&mut self, so_timeout: u32) {
        self.so_timeout = so_timeout;
    }
    pub fn set_conn_timeout(&mut self, conn_timeout: u32) {
        self.conn_timeout = conn_timeout;
    }
    pub fn set_receive_buffer_size(&mut self, receive_buffer_size: u32) {
        self.receive_buffer_size = receive_buffer_size;
    }
    pub fn set_send_buffer_size(&mut self, send_buffer_size: u32) {
        self.send_buffer_size = send_buffer_size;
    }
    pub fn set_channel(&mut self, channel: Option<Box<dyn TcpSocketChannel>>) {
        self.channel = channel;
    }
    pub fn set_dumping(&mut self, dumping: bool) {
        self.dumping = dumping;
    }
    pub fn set_connection_id(&mut self, connection_id: u32) {
        self.connection_id = connection_id;
    }
    pub fn set_connected(&mut self, connected: AtomicBool) {
        self.connected = connected;
    }
    pub fn set_server_version(&mut self, server_version: String) {
        self.server_version = server_version;
    }
    pub fn set_timeout(&mut self, timeout: u32) {
        self.timeout = timeout;
    }
}


trait ErosaConnection {
    fn connect(&mut self);
    fn reconnect(&mut self);
    fn disconnect(&mut self);
    fn seek<E>(&mut self, binlog_filename: &str, binlog_position: i64, gtid: &str, sink_function: Box<dyn SinkFunction<E>>);
    fn at_dump_sink<E>(&mut self, binlog_filename: &str, binlog_position: i64, sink_function: Box<dyn SinkFunction<E>>);
    fn at_dump_timestamp<E>(&mut self, timestamp: i64, sink_function: Box<dyn SinkFunction<E>>);
    fn at_dump_file_coprocessor(&mut self, binlog_filename: &str, binlog_position: i64, coprocessor: Box<dyn MultiStageCoprocessor>);
    fn at_dump_timestamp_coprocessor(&mut self, timestamp: i64, coprocessor: Box<dyn MultiStageCoprocessor>);
    fn fork(&self) -> Self;
    fn query_server_id(&self) -> i64;
}

enum BinlogFormat {
    STATEMENT,
    ROW,
    MIXED,
    None,
}

enum BinlogImage {
    FULL,
    MINIMAL,
    NOBLOB,
    None,
}

const connTimeout: u32 = 5 * 1000;
const soTimeout: u32 = 60 * 60 * 1000;
const binlogChecksum: u32 = log_event::BINLOG_CHECKSUM_ALG_OFF as u32;

struct MysqlConnection<'a> {
    connector: MysqlConnector,
    slave_id: i64,
    charset: Option<Chars<'a>>,
    binlog_format: BinlogFormat,
    binlog_image: BinlogImage,
    authInfo: AuthenticationInfo,
    receivedBinlogBytes: AtomicI64,
}


impl<'a> MysqlConnection<'a> {
    pub fn from(address: String, port: u16, username: String, password: String) -> MysqlConnection<'a> {
        let mut connection = MysqlConnection::new();
        connection.init(address,port, username, password)
    }

    pub fn from_schema(address: String, port: u16, username: String, password: String, default_schema: String) -> MysqlConnection<'a> {
        let mut connection = MysqlConnection::new();
        connection.init_schema(address,port, username, password, default_schema)
    }
    fn init(mut self, address: String, port: u16, username: String, password: String) -> MysqlConnection<'a> {
        self.connector.set_address(address.clone());
        self.connector.set_port(port);
        self.connector.set_password(password.clone());
        self.connector.set_username(username.clone());
        self.authInfo.set_password(password);
        self.authInfo.set_username(username);
        self.authInfo.set_port(port);
        self.authInfo.set_address(address);
        self
    }
    fn init_schema(mut self, address: String, port: u16, username: String, password: String, schema: String) -> MysqlConnection<'a> {
        self.connector.set_address(address.clone());
        self.connector.set_port(port);
        self.connector.set_password(password.clone());
        self.connector.set_username(username.clone());
        self.connector.set_default_schema(schema.clone());
        self.authInfo.set_password(password);
        self.authInfo.set_default_database_name(schema);
        self.authInfo.set_username(username);
        self.authInfo.set_port(port);
        self.authInfo.set_address(address);
        self
    }


    pub fn new() -> Self {
        Self {
            connector: MysqlConnector::new(),
            slave_id: 0,
            charset: Option::None,
            binlog_format: BinlogFormat::None,
            binlog_image: BinlogImage::None,
            authInfo: AuthenticationInfo::new(),
            receivedBinlogBytes: Default::default(),
        }
    }
}

impl<'a> ErosaConnection for MysqlConnection<'a> {
    fn connect(&mut self) {
        self.connector.connect();
    }

    fn reconnect(&mut self) {
        todo!()
    }

    fn disconnect(&mut self) {
        todo!()
    }

    fn seek<E>(&mut self, binlog_filename: &str, binlog_position: i64, gtid: &str, sink_function: Box<dyn SinkFunction<E>>) {
        todo!()
    }

    fn at_dump_sink<E>(&mut self, binlog_filename: &str, binlog_position: i64, sink_function: Box<dyn SinkFunction<E>>) {
        todo!()
    }

    fn at_dump_timestamp<E>(&mut self, timestamp: i64, sink_function: Box<dyn SinkFunction<E>>) {
        todo!()
    }

    fn at_dump_file_coprocessor(&mut self, binlog_filename: &str, binlog_position: i64, coprocessor: Box<dyn MultiStageCoprocessor>) {
        todo!()
    }

    fn at_dump_timestamp_coprocessor(&mut self, timestamp: i64, coprocessor: Box<dyn MultiStageCoprocessor>) {
        todo!()
    }

    fn fork(&self) -> Self {
        todo!()
    }

    fn query_server_id(&self) -> i64 {
        todo!()
    }
}






