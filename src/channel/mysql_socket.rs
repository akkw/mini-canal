use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::str::{from_utf8};
use crate::channel::{TcpChannel, TcpSocketChannel};
use crate::channel::read_write_packet::{*};
use crate::channel::sql_utils::{MysqlQueryExecutor, MysqlUpdateExecutor};
use crate::command::{msc, Packet, write_header_and_body};
use crate::command::server::{*};
use crate::utils::mysql_password_encrypted::{*};
use crate::command::client::{*};
use crate::log::decoder::LogDecoder;
use crate::log::event::{Event, FormatDescriptionLogEvent, LogContext};
use crate::log::log_buffer::DirectLogFetcher;
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

            self.negotiate().unwrap();
        } else if result.is_err() {
            println!("the connection is already established")
        }
    }

    pub fn disconnect(&mut self) {
        let result = self.connected.compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed);
        if result.is_ok() {
            if let Some(channel) = self.channel() {
                channel.as_ref().close().unwrap();
            }


            if self.dumping && self.connection_id > 0 {
                let mut connector = self.fork();
                connector.connect();
                let id = self.connection_id;
                let mut executor = MysqlUpdateExecutor::new(self);
                executor.update(&format!("KILL CONNECTION {}", id)).unwrap();

                self.dumping = false;
            }
        }
    }

    pub fn reconnect(&mut self) {
        self.disconnect();
        self.connect();
    }
    fn fork(&self) -> MysqlConnector {
        MysqlConnector::new_schema(self.address.clone(), self.port, self.username.clone(), self.password.as_ref().unwrap().clone(), 33, self.default_schema.clone())
    }

    fn negotiate(&mut self) -> Result<bool, String> {
        let header = read_header_timeout(self.channel.as_mut().unwrap(), TIMEOUT).unwrap();
        let body = read_bytes(self.channel.as_mut().unwrap(), header.packet_body_length());


        if body[0] > 127 {
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
            let auth_data;
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
                encrypted_password = scramble_caching_sha2(self.password.as_ref().unwrap().as_bytes(), auth_data);
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

    fn join_and_create_scrumble_buff(&self, handshake_packet: &HandshakeInitializationPacket) -> Box<[u8]> {
        let mut out = vec![];
        for i in 0..handshake_packet.seed().len() {
            out.push(handshake_packet.seed()[i])
        }
        for i in 0..handshake_packet.rest_of_scramble_buff().len() {
            out.push(handshake_packet.rest_of_scramble_buff()[i])
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
    pub fn channel_as_mut(&mut self) -> &mut Option<Box<dyn TcpSocketChannel>> {
        &mut self.channel
    }

    pub fn channel(&self) -> &Option<Box<dyn TcpSocketChannel>> {
        &self.channel
    }
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

const BINLOG_CHECKSUM: u32 = Event::BINLOG_CHECKSUM_ALG_OFF as u32;

pub struct MysqlConnection {
    connector: MysqlConnector,
    slave_id: u32,
    binlog_format: BinlogFormat,
    binlog_image: BinlogImage,
    auth_info: AuthenticationInfo,
    received_binlog_bytes: AtomicI64,
    binlog_checksum: u8,
}


trait SqlProcess {
    fn query(&mut self, sql: &str) -> ResultSetPacket;
    fn query_multi(&mut self, sql: &str) -> Vec<ResultSetPacket>;
    fn update(&mut self, sql: &str) -> u32;
}

impl MysqlConnection {
    pub fn from(address: String, port: u16, username: String, password: String) -> MysqlConnection {
        let connection = MysqlConnection::new();
        connection.init(address, port, username, password)
    }

    pub fn from_schema(address: String, port: u16, username: String, password: String, default_schema: String) -> MysqlConnection {
        let connection = MysqlConnection::new();
        connection.init_schema(address, port, username, password, default_schema)
    }
    fn init(mut self, address: String, port: u16, username: String, password: String) -> MysqlConnection {
        self.connector.set_address(address.clone());
        self.connector.set_port(port);
        self.connector.set_password(password.clone());
        self.connector.set_username(username.clone());
        self.auth_info.set_password(password);
        self.auth_info.set_username(username);
        self.auth_info.set_port(port);
        self.auth_info.set_address(address);
        self
    }
    fn init_schema(mut self, address: String, port: u16, username: String, password: String, schema: String) -> MysqlConnection {
        self.connector.set_address(address.clone());
        self.connector.set_port(port);
        self.connector.set_password(password.clone());
        self.connector.set_username(username.clone());
        self.connector.set_default_schema(schema.clone());
        self.auth_info.set_password(password);
        self.auth_info.set_default_database_name(schema);
        self.auth_info.set_username(username);
        self.auth_info.set_port(port);
        self.auth_info.set_address(address);
        self
    }


    pub fn new() -> Self {
        Self {
            connector: MysqlConnector::new(),
            slave_id: 0,
            binlog_format: BinlogFormat::ROW,
            binlog_image: BinlogImage::FULL,
            auth_info: AuthenticationInfo::new(),
            received_binlog_bytes: Default::default(),
            binlog_checksum: Event::BINLOG_CHECKSUM_ALG_OFF,
        }
    }


    pub fn connector(&mut self) -> &mut MysqlConnector {
        &mut self.connector
    }

    pub fn connect(&mut self) {
        self.connector.connect();
    }

    pub fn reconnect(&mut self) {
        self.connector.reconnect()
    }

    pub fn disconnect(&mut self) {
        self.connector.disconnect()
    }

    pub fn at_dump_sink(&mut self, binlog_filename: &str, binlog_position: u32) {
        self.update_settings();
        self.load_binlog_checksum();
        self.send_register_slave();
        self.send_binlog_dump(binlog_filename, binlog_position);
        let mut fetcher = DirectLogFetcher::new();
        let channel = &mut self.connector.channel.as_mut().unwrap();
        fetcher.start(Option::Some(channel));

        let decoder = LogDecoder::from(Event::UNKNOWN_EVENT as usize, Event::ENUM_END_EVENT);
        let mut context = LogContext::new();
        context.set_description_event(FormatDescriptionLogEvent::from_binlog_version_binlog_check_sum(4, self.binlog_checksum));
        while fetcher.fetch().unwrap() {
            let mut event = decoder.decode(fetcher.log_buffer(), &mut context);
            println!("event: {:?}", event);


            let semival = event.event_mut().unwrap();
            if semival.semival() == 1 {
                println!("send semival!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            }
        }
    }

    fn update_settings(&mut self) {
        self.update("set wait_timeout=9999999");
        self.update("set net_write_timeout=7200");
        self.update("set net_read_timeout=7200");
        self.update("set names 'binary'");
        self.update("set @master_binlog_checksum= @@global.binlog_checksum");
        self.update("set @slave_uuid=uuid()");
        self.update(format!("SET @mariadb_slave_capability= '{}'", Event::MARIA_SLAVE_CAPABILITY_MINE).as_str());
        self.update(format!("SET @master_heartbeat_period=  '{}'", DirectLogFetcher::MASTER_HEARTBEAT_PERIOD_NANOSECOND).as_str());
    }


    fn load_binlog_checksum(&mut self) {
        let packet = self.query("select @@global.binlog_checksum");
        let col_values = packet.field_values();
        if col_values.len() >= 1 && col_values.get(0).unwrap().len() != 0
            && col_values.get(0).unwrap().to_ascii_uppercase().eq("CRC32") {
            self.binlog_checksum = Event::BINLOG_CHECKSUM_ALG_CRC32
        } else {
            self.binlog_checksum = Event::BINLOG_CHECKSUM_ALG_OFF
        }
    }

    fn send_register_slave(&mut self) {
        let v4 = self.connector.channel.as_mut().unwrap().get_local_address().unwrap();
        let host = v4.ip().to_string();
        let port = v4.port();
        let mut packet = RegisterSlaveCommandPacket::from(host.as_str(), port, self.auth_info.username(), self.auth_info.password(), self.slave_id);
        let body = packet.to_bytes();
        println!("Register slave");
        write_body(self.connector.channel.as_mut().unwrap(), &body);

        let header = read_header(self.connector.channel.as_mut().unwrap()).unwrap();
        let bytes = read_bytes(self.connector.channel.as_mut().unwrap(), header.packet_body_length());

        if bytes[0] > 127 {
            if bytes[0] == 255 {
                let mut error = ErrorPacket::new();
                error.from_bytes(&body);
                println!("{}", format!("Error When doing Register slave: {}", error))
            } else {
                println!("{}", format!("Unexpected packet"))
            }
        }
    }


    fn send_binlog_dump(&mut self, binlog_filename: &str, binlog_position: u32) {
        let mut packet = BinlogDumpCommandPacket::from(binlog_filename, binlog_position, self.slave_id);
        let body = packet.to_bytes();

        println!("COM_BINLOG_DUMP with position:{}", packet);
        write_body(self.connector.channel.as_mut().unwrap(), &body)
    }

    pub fn fork(&self) -> Self {
        let mut connection = MysqlConnection::new();
        connection.set_slave_id(self.slave_id);
        connection.set_connector(self.connector.fork());
        connection.set_auth_info(self.auth_info.clone());

        connection
    }

    pub fn query_server_id(&mut self) -> i64 {
        let packet = self.query("show variables like 'server_id'");
        let values = packet.field_values();
        if values.len() != 2 {
            return 0;
        }
        let x = values.get(1).unwrap();
        let server_id = x.parse::<i64>().unwrap();
        server_id
    }


    pub fn set_slave_id(&mut self, slave_id: u32) {
        self.slave_id = slave_id;
    }
    pub fn set_connector(&mut self, connector: MysqlConnector) {
        self.connector = connector;
    }
    pub fn set_auth_info(&mut self, auth_info: AuthenticationInfo) {
        self.auth_info = auth_info;
    }


    pub fn query(&mut self, sql: &str) -> ResultSetPacket {
        let mut query_packet = MysqlQueryExecutor::from(&mut self.connector);
        let result = query_packet.query(sql);
        let packet = result.unwrap();
        packet
    }
}


impl SqlProcess for MysqlConnection {
    fn query(&mut self, sql: &str) -> ResultSetPacket {
        let mut executor = MysqlQueryExecutor::from(self.connector());
        executor.query(sql).unwrap()
    }

    fn query_multi(&mut self, sql: &str) -> Vec<ResultSetPacket> {
        let mut executor = MysqlQueryExecutor::from(self.connector());
        executor.query_multi(sql).unwrap()
    }

    fn update(&mut self, sql: &str) -> u32 {
        let mut executor = MysqlUpdateExecutor::new(self.connector());
        executor.update(sql).unwrap()
    }
}






