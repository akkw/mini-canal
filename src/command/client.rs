use crate::command::{*};
use crate::utils::mysql_password_encrypted::scramble411;

pub struct AuthSwitchResponsePacket<'a> {
    command: u8,
    auth_data: &'a [u8],
}

impl<'a> AuthSwitchResponsePacket<'a> {
    pub fn command(&self) -> u8 {
        self.command
    }
    pub fn auth_data(&self) -> &'a [u8] {
        self.auth_data
    }

    pub fn set_command(&mut self, command: u8) {
        self.command = command;
    }
    pub fn set_auth_data(&mut self, auth_data: &'a [u8]) {
        self.auth_data = auth_data;
    }


    pub fn new() -> Self {
        Self { command: 0, auth_data: &[] }
    }
}


impl<'a, 'b : 'a> Packet<'b> for AuthSwitchResponsePacket<'a> {
    fn from_bytes(&mut self, buf: &'b [u8]) {
        todo!()
    }

    fn to_bytes(&mut self) -> Box<[u8]> {
        let mut data = vec![];
        for i in 0..self.auth_data.len() {
            data.push(self.auth_data[i])
        }
        Box::from(data)
    }
}

const BINLOG_DUMP_NON_BLOCK: u32 = 1;
const BINLOG_SEND_ANNOTATE_ROWS_EVENT: u32 = 2;

pub struct BinlogDumpCommandPacket<'a> {
    command: u8,
    binlog_dump_non_block: u32,
    binlog_send_annotate_rows_event: u32,
    binlog_position: u32,
    slave_server_id: u32,
    binlog_file_name: &'a str,
}

/**
 * <pre>
 * Bytes                        Name
 *  -----                        ----
 *  1                            command
 *  n                            arg
 *  --------------------------------------------------------
 *  Bytes                        Name
 *  -----                        ----
 *  4                            binlog position to start at (little endian)
 *  2                            binlog flags (currently not used; always 0)
 *  4                            server_id of the slave (little endian)
 *  n                            binlog file name (optional)
 *
 * </pre>
 */
impl<'a> BinlogDumpCommandPacket<'a> {
    pub fn _new() -> Self {
        Self {
            command: 0,
            binlog_dump_non_block: BINLOG_DUMP_NON_BLOCK,
            binlog_send_annotate_rows_event: BINLOG_SEND_ANNOTATE_ROWS_EVENT,
            binlog_position: 0,
            slave_server_id: 0,
            binlog_file_name: "",
        }
    }


    pub fn _command(&self) -> u8 {
        self.command
    }
    pub fn _binlog_dump_non_block(&self) -> u32 {
        self.binlog_dump_non_block
    }
    pub fn _binlog_send_annotate_rows_event(&self) -> u32 {
        self.binlog_send_annotate_rows_event
    }
    pub fn _binlog_position(&self) -> u32 {
        self.binlog_position
    }
    pub fn _slave_server_id(&self) -> u32 {
        self.slave_server_id
    }
    pub fn _binlog_file_name(&self) -> &'a str {
        self.binlog_file_name
    }


    pub fn _set_command(&mut self, command: u8) {
        self.command = command;
    }
    pub fn _set_binlog_dump_non_block(&mut self, binlog_dump_non_block: u32) {
        self.binlog_dump_non_block = binlog_dump_non_block;
    }
    pub fn _set_binlog_send_annotate_rows_event(&mut self, binlog_send_annotate_rows_event: u32) {
        self.binlog_send_annotate_rows_event = binlog_send_annotate_rows_event;
    }
    pub fn _set_binlog_position(&mut self, binlog_position: u32) {
        self.binlog_position = binlog_position;
    }
    pub fn _set_slave_server_id(&mut self, slave_server_id: u32) {
        self.slave_server_id = slave_server_id;
    }
    pub fn _set_binlog_file_name(&mut self, binlog_file_name: &'a str) {
        self.binlog_file_name = binlog_file_name;
    }
}


impl<'a, 'b: 'a> Packet<'b> for BinlogDumpCommandPacket<'a> {
    fn from_bytes(&mut self, buf: &'b [u8]) {
        todo!()
    }

    fn to_bytes(&mut self) -> Box<[u8]> {
        let mut out = vec![];
        out.push(self.command);
        write_unsigned_4byte_little_endian(self.binlog_position, &mut out);
        let mut binlog_flags: u8 = 0;
        binlog_flags |= BINLOG_SEND_ANNOTATE_ROWS_EVENT as u8;
        out.push(binlog_flags);
        out.push(0x00);
        write_unsigned_4byte_little_endian(self.slave_server_id, &mut out);
        if self.binlog_file_name.len() != 0 {
            let binlog_file_name_bytes = self.binlog_file_name.as_bytes();
            for i in 0..binlog_file_name_bytes.len() {
                out.push(binlog_file_name_bytes[i])
            }
        }
        Box::from(out)
    }
}

struct BinlogDumpGTIDCommandPacket {}


pub struct ClientAuthenticationPacket<'a> {
    header: HeaderPacket,
    client_capability: i32,
    username: &'a str,
    password: &'a str,
    charset_number: u8,
    database_name: &'a str,
    server_capabilities: u16,
    scrumble_buff: &'a [u8],
    auth_plugin_name: &'a [u8],
}

const CLIENT_CAPABILITY: i32 = capability::CLIENT_LONG_PASSWORD | capability::CLIENT_LONG_FLAG
    | capability::CLIENT_PROTOCOL_41 | capability::CLIENT_INTERACTIVE
    | capability::CLIENT_TRANSACTIONS | capability::CLIENT_SECURE_CONNECTION
    | capability::CLIENT_MULTI_STATEMENTS | capability::CLIENT_PLUGIN_AUTH;

impl<'a> ClientAuthenticationPacket<'a> {
    pub fn new() -> Self {
        Self {
            header: HeaderPacket::new(),
            client_capability: CLIENT_CAPABILITY,
            username: "",
            password: "",
            charset_number: 0,
            database_name: "",
            server_capabilities: 0,
            scrumble_buff: &[],
            auth_plugin_name: &[],
        }
    }


    pub fn _header(&self) -> &HeaderPacket {
        &self.header
    }
    pub fn _client_capability(&self) -> i32 {
        self.client_capability
    }
    pub fn _username(&self) -> &'a str {
        self.username
    }
    pub fn _password(&self) -> &'a str {
        self.password
    }
    pub fn _charset_number(&self) -> u8 {
        self.charset_number
    }
    pub fn _database_name(&self) -> &'a str {
        self.database_name
    }
    pub fn _server_capabilities(&self) -> u16 {
        self.server_capabilities
    }
    pub fn _scrumble_buff(&self) -> &'a [u8] {
        self.scrumble_buff
    }
    pub fn _auth_plugin_name(&self) -> &'a [u8] {
        self.auth_plugin_name
    }


    pub fn _set_header(&mut self, header: HeaderPacket) {
        self.header = header;
    }
    pub fn _set_client_capability(&mut self, client_capability: i32) {
        self.client_capability = client_capability;
    }
    pub fn set_username(&mut self, username: &'a str) {
        self.username = username;
    }
    pub fn set_password(&mut self, password: &'a str) {
        self.password = password;
    }
    pub fn set_charset_number(&mut self, charset_number: u8) {
        self.charset_number = charset_number;
    }
    pub fn set_database_name(&mut self, database_name: &'a str) {
        self.database_name = database_name;
        self.client_capability |= capability::CLIENT_CONNECT_WITH_DB;
    }
    pub fn set_server_capabilities(&mut self, server_capabilities: u16) {
        self.server_capabilities = server_capabilities;
    }
    pub fn set_scrumble_buff(&mut self, scrumble_buff: &'a [u8]) {
        self.scrumble_buff = scrumble_buff;
    }
    pub fn set_auth_plugin_name(&mut self, auth_plugin_name: &'a [u8]) {
        self.auth_plugin_name = auth_plugin_name;
        self.client_capability |= capability::CLIENT_PLUGIN_AUTH;
    }
}

impl<'a, 'b: 'a> Packet<'b> for ClientAuthenticationPacket<'a> {
    fn from_bytes(&mut self, buf: &'a [u8]) {
        todo!()
    }
    /**
     * <pre>
     * VERSION 4.1
     *  Bytes                        Name
     *  -----                        ----
     *  4                            client_flags
     *  4                            max_packet_size
     *  1                            charset_number
     *  23                           (filler) always 0x00...
     *  n (Null-Terminated String)   user
     *  n (Length Coded Binary)      scramble_buff (1 + x bytes)
     *  n (Null-Terminated String)   databasename (optional)
     *  n (Null-Terminated String)   auth plugin name (optional)
     * </pre>
     *
     */
    fn to_bytes(&mut self) -> Box<[u8]> {
        let mut out = vec![];
        // 1. write client_flags
        write_unsigned_4byte_little_endian(self.client_capability as u32, &mut out);
        write_unsigned_4byte_little_endian(msc::MAX_PACKET_LENGTH, &mut out);
        out.push(self.charset_number);

        for i in 0..23 {
            out.push(0);
        }


        write_null_terminated_string(self.username, &mut out);


        if self.password.len() == 0 {
            out.push(0x00);
        } else {
            let scramble4111_body = scramble411(self.password.as_bytes(), self.scrumble_buff);
            write_binary_coded_length_bytes(&scramble4111_body, &mut out);
        }

        if self.database_name.len() != 0 {
            write_null_terminated_string(self.database_name, &mut out);
        }

        if self.auth_plugin_name.len() != 0 {
            write_null_terminated(self.auth_plugin_name, &mut out);
        }

        Box::from(out)
    }
}

const QUERY_COMMAND: u8 = 0x03;

pub struct QueryCommandPacket<'a> {
    command: u8,
    sql: &'a str,
}


impl<'a> QueryCommandPacket<'a> {
    pub fn from(sql: &'a str) -> Self {
        Self { command: QUERY_COMMAND, sql }
    }
}

impl<'a, 'b: 'a> Packet<'b> for QueryCommandPacket<'a> {
    fn from_bytes(&mut self, buf: &'b [u8]) {}

    fn to_bytes(&mut self) -> Box<[u8]> {
        let mut out = vec![];
        out.push(self.command);
        let bytes = self.sql.as_bytes();
        for i in 0..bytes.len() {
            out.push(bytes[i]);
        }
        Box::from(out)
    }
}

const QUIT_COMMAND: u8 = 0x01;

pub struct QuitCommandPacket {
    command: u8,
}

impl QuitCommandPacket {
    pub fn new() -> Self {
        Self { command: QUIT_COMMAND }
    }
}

impl<'a> Packet<'a> for QuitCommandPacket {
    fn from_bytes(&mut self, buf: &'a [u8]) {
        todo!()
    }

    fn to_bytes(&mut self) -> Box<[u8]> {
        let mut out = vec![];
        out.push(self.command);
        Box::from(out)
    }
}

const REGISTER_SLAVE_COMMAND_PACKET: u8 = 0x15;

pub struct RegisterSlaveCommandPacket<'a> {
    command: u8,
    report_host: &'a str,
    report_port: u16,
    report_user: &'a str,
    report_passwd: &'a str,
    server_id: u32,
}

impl<'a> RegisterSlaveCommandPacket<'a> {
    pub fn new(report_host: &'a str, report_port: u16, report_user: &'a str, report_passwd: &'a str, server_id: u32) -> Self {
        Self { command: REGISTER_SLAVE_COMMAND_PACKET, report_host, report_port, report_user, report_passwd, server_id }
    }
}

impl<'a, 'b: 'a> Packet<'b> for RegisterSlaveCommandPacket<'a> {
    fn from_bytes(&mut self, buf: &'b [u8]) {
        todo!()
    }

    fn to_bytes(&mut self) -> Box<[u8]> {
        let mut out = vec![];
        out.push(self.command);
        write_unsigned_4byte_little_endian(self.server_id, &mut out);
        out.push(self.report_host.len() as u8);
        write_fixed_length_bytes_from_start(self.report_host.as_bytes(), self.report_host.len(), &mut out);
        out.push(self.report_user.len() as u8);
        write_fixed_length_bytes_from_start(self.report_user.as_bytes(), self.report_user.len(), &mut out);
        out.push(self.report_passwd.len() as u8);
        write_fixed_length_bytes_from_start(self.report_passwd.as_bytes(), self.report_passwd.len(), &mut out);
        write_unsigned_2byte_little_endian_vec(self.report_port, &mut out);
        write_unsigned_4byte_little_endian(0, &mut out);// Fake
        // rpl_recovery_rank
        write_unsigned_4byte_little_endian(0, &mut out);
        Box::from(out)
    }
}

const SEMI_ACK_COMMAND_PACKET: u8 = 0xef;
pub struct SemiAckCommandPacket<'a> {
    command: u8,
    binlog_position: u64,
    binlog_file_name: &'a str,
}

impl<'a> SemiAckCommandPacket<'a> {
    pub fn new(binlog_position: u64, binlog_file_name: &'a str) -> Self {
        Self { command: SEMI_ACK_COMMAND_PACKET, binlog_position, binlog_file_name }
    }
}

impl < 'a,  'b: 'a >  Packet <'b>for SemiAckCommandPacket<'a> {
    fn from_bytes(&mut self, buf: &'b [u8]) {
        todo!()
    }

    fn to_bytes(&mut self) -> Box<[u8]> {
        let mut out = vec![];
        out.push(SEMI_ACK_COMMAND_PACKET);
        write_unsigned_8byte_little_endian(self.binlog_position, &mut out);
        if self.binlog_file_name.len() != 0 {
            write_fixed_length_bytes_from_start(self.binlog_file_name.as_bytes(), self.binlog_file_name.len(),&mut out);
        }
        Box::from(out)
    }


}