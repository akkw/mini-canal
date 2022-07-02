use std::borrow::Borrow;
use std::slice::SliceIndex;
use std::str::{*};
use msc::{*};
use capability::{*};


pub mod packet_utils {}

pub mod msc {
    pub const DEFAULT_PROTOCOL_VERSION: u8 = 0x0a;
    pub const NULL_TERMINATED_STRING_DELIMITER: u8 = 0x00;
}

pub mod capability {
    // Use the improved version of Old Password Authentication.
    // Assumed to be set since 4.1.1.
    pub const CLIENT_LONG_PASSWORD: i32 = 0x00000001;

    // Send found rows instead of affected rows in EOF_Packet.
    pub const CLIENT_FOUND_ROWS: i32 = 0x00000002;

    // https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition320
    // Longer flags in Protocol::ColumnDefinition320.
    // Server:Supports longer flags.
    // Client:Expects longer flags.
    // 执行查询sql时，除了返回结果集，还返回元数据
    pub const CLIENT_LONG_FLAG: i32 = 0x00000004;

    // 可以在handshake时，指定一个数据库名
    // Database (schema) name can be specified on connect in Handshake Response
    // Packet.
    // Server: Supports schema-name in Handshake Response Packet.
    // Client: Handshake Response Packet contains a schema-name.
    pub const CLIENT_CONNECT_WITH_DB: i32 = 0x00000008;

    // Server: Do not permit database.table.column.
    pub const CLIENT_NO_SCHEMA: i32 = 0x00000010;

    // Compression protocol supported.
    // Server:Supports compression.
    // Client:Switches to Compression compressed protocol after successful
    // authentication.
    pub const CLIENT_COMPRESS: i32 = 0x00000020;

    // Special handling of ODBC behavior.
    // No special behavior since 3.22.
    pub const CLIENT_ODBC: i32 = 0x00000040;

    // Can use LOAD DATA LOCAL.
    // Server:Enables the LOCAL INFILE request of LOAD DATA|XML.
    // Client:Will handle LOCAL INFILE request.
    pub const CLIENT_LOCAL_FILES: i32 = 0x00000080;

    // Server: Parser can ignore spaces before '('.
    // Client: Let the parser ignore spaces before '('.
    pub const CLIENT_IGNORE_SPACE: i32 = 0x00000100;

    // Server:Supports the 4.1 protocol,
    // 4.1协议中，
    // OKPacket将会包含warning count
    // ERR_Packet包含SQL state
    // EOF_Packet包含warning count和status flags
    // Client:Uses the 4.1 protocol.
    // Note: this value was CLIENT_CHANGE_USER in 3.22, unused in 4.0
    // If CLIENT_PROTOCOL_41 is set：
    // 1、the ok packet contains a warning count.
    // https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
    // 2、ERR_Packet It contains a SQL state value if CLIENT_PROTOCOL_41 is
    // enabled. //https://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html
    // 3、EOF_Packet If CLIENT_PROTOCOL_41 is enabled, the EOF packet contains a
    // warning count and status flags.
    // https://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
    pub const CLIENT_PROTOCOL_41: i32 = 0x00000200;

    // wait_timeout versus wait_interactive_timeout.
    // Server:Supports interactive and noninteractive clients.
    // Client:Client is interactive.
    pub const CLIENT_INTERACTIVE: i32 = 0x00000400;

    // Server: Supports SSL.
    // Client: Switch to SSL after sending the capability-flags.
    pub const CLIENT_SSL: i32 = 0x00000800;

    // Client: Do not issue SIGPIPE if network failures occur (libmysqlclient
    // only).
    pub const CLIENT_IGNORE_SIGPIPE: i32 = 0x00001000;

    // Server: Can send status flags in EOF_Packet.
    // Client:Expects status flags in EOF_Packet.
    // Note:This flag is optional in 3.23, but always set by the server since
    // 4.0.
    pub const CLIENT_TRANSACTIONS: i32 = 0x00002000;

    // Unused
    // Note: Was named CLIENT_PROTOCOL_41 in 4.1.0.
    pub const CLIENT_RESERVED: i32 = 0x00004000;

    /**
     * <pre>
     *      服务端返回20 byte随机字节，客户端利用其对密码进行加密，加密算法如下：
     *      https://dev.mysql.com/doc/internals/en/secure-password-authentication.html#packet-Authentication::Native41
     *      Authentication::Native41:
     *      client-side expects a 20-byte random challenge
     *      client-side returns a 20-byte response based on the algorithm described later
     *      Name
     *      mysql_native_password
     *      Requires
     *      CLIENT_SECURE_CONNECTION
     *      Image description follows.
     *      Image description
     *      This method fixes a 2 short-comings of the Old Password Authentication:
     *      (https://dev.mysql.com/doc/internals/en/old-password-authentication.html#packet-Authentication::Old)
     *      using a tested, crypto-graphic hashing function which isn't broken
     *      knowning the content of the hash in the mysql.user table isn't enough to authenticate against the MySQL Server.
     *      The password is calculated by:
     *      SHA1( password ) XOR SHA1( "20-bytes random data from server" <concat> SHA1( SHA1( password ) ) )
     * </pre>
     */
    pub const CLIENT_SECURE_CONNECTION: i32 = 0x00008000;

    // Server:Can handle multiple statements per COM_QUERY and COM_STMT_PREPARE.
    // Client:May send multiple statements per COM_QUERY and COM_STMT_PREPARE.
    // Note:Was named CLIENT_MULTI_QUERIES in 4.1.0, renamed later.
    // Requires:CLIENT_PROTOCOL_41
    pub const CLIENT_MULTI_STATEMENTS: i32 = 0x00010000;

    // Server: Can send multiple resultsets for COM_QUERY.
    // Client: Can handle multiple resultsets for COM_QUERY.
    // Requires:CLIENT_PROTOCOL_41
    pub const CLIENT_MULTI_RESULTS: i32 = 0x00020000;

    // Server: Can send multiple resultsets for ComStmtExecutePacket.
    // Client: Can handle multiple resultsets for ComStmtExecutePacket.
    // Requires:CLIENT_PROTOCOL_41
    pub const CLIENT_PS_MULTI_RESULTS: i32 = 0x00040000;

    // Server:Sends extra data in Initial Handshake Packet and supports the
    // pluggable authentication protocol.
    // Client: Supports authentication plugins.
    // Requires: CLIENT_PROTOCOL_41
    pub const CLIENT_PLUGIN_AUTH: i32 = 0x00080000;

    // Server: Permits connection attributes in Protocol::HandshakeResponse41.
    // Client: Sends connection attributes in Protocol::HandshakeResponse41.
    pub const CLIENT_CONNECT_ATTRS: i32 = 0x00100000;

    // Server:Understands length-encoded integer for auth response data in
    // Protocol::HandshakeResponse41.
    // Client:Length of auth response data in Protocol::HandshakeResponse41 is a
    // length-encoded integer.
    // Note: The flag was introduced in 5.6.6, but had the wrong value.
    pub const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: i32 = 0x00200000;

    // Server: Announces support for expired password extension.
    // Client: Can handle expired passwords.
    pub const CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS: i32 = 0x00400000;

    // Server: Can set SERVER_SESSION_STATE_CHANGED in the Status Flags and send
    // session-state change data after a OK packet.
    // Client: Expects the server to send sesson-state changes after a OK
    // packet.
    pub const CLIENT_SESSION_TRACK: i32 = 0x00800000;

    /**
     * Server: Can send OK after a Text Resultset. Client: Expects an OK
     * (instead of EOF) after the resultset rows of a Text Resultset.
     * Background:To support CLIENT_SESSION_TRACK, additional information must
     * be sent after all successful commands. Although the OK packet is
     * extensible, the EOF packet is not due to the overlap of its bytes with
     * the content of the Text Resultset Row. Therefore, the EOF packet in the
     * Text Resultset is replaced with an OK packet. EOF packets are deprecated
     * as of MySQL 5.7.5.
     */
    pub const CLIENT_DEPRECATE_EOF: i32 = 0x01000000;
}

trait Packet<'a> {
    fn from_bytes(&mut self, buf: &'a [u8]);
    fn to_bytes(&mut self) -> Box<[u8]>;
}


struct AuthSwitchRequestMoreData<'a> {
    command: u8,
    status: i32,
    auth_data: &'a [u8],
}

impl<'a> AuthSwitchRequestMoreData<'a> {
    fn set_command(&mut self, command: u8) {
        self.command = command
    }

    fn get_command(&self) -> u8 {
        self.command
    }
}

impl<'a, 'b: 'a> Packet<'b> for AuthSwitchRequestMoreData<'a> {
    fn from_bytes(&mut self, buf: &'b [u8]) {
        let mut index = 0;
        self.status = buf[index] as i32;
        index += 1;
        self.auth_data = read_none_terminated_bytes(&buf);
    }

    fn to_bytes(&mut self) -> Box<[u8]> {
        todo!()
    }
}

struct AuthSwitchRequestPacket<'a> {
    command: u8,
    auth_name: &'a str,
    auth_data: &'a [u8],
}

impl<'a> AuthSwitchRequestPacket<'a> {
    fn set_command(&mut self, command: u8) {
        self.command = command;
    }

    fn get_command(&self) -> u8 {
        self.command
    }
}

impl<'a, 'b: 'a> Packet<'b> for AuthSwitchRequestPacket<'a> {
    fn from_bytes(&mut self, buf: &'b [u8]) {
        let mut index = 0;
        self.command = buf[index];
        index += 1;
        let auth_name = read_none_terminated_bytes(&buf[index..]);
        self.auth_name = from_utf8(auth_name).unwrap();
        index = auth_name.len() + 1;

        self.auth_data = read_none_terminated_bytes(&buf[index..])
    }

    fn to_bytes(&mut self) -> Box<[u8]> {
        todo!()
    }
}

struct HeaderPacket {
    packet_body_length: i32,
    packet_sequence_number: u8,
}

impl<'b> Packet<'b> for HeaderPacket {
    #[allow(arithmetic_overflow)]
    fn from_bytes(&mut self, buf: &[u8]) {
        self.packet_body_length = ((buf[0] & 0xFF) | ((buf[1] & 0xFF) << 8) | ((buf[2] & 0xFF) << 16)) as i32;
        self.packet_sequence_number = buf[3];
    }

    fn to_bytes(&mut self) -> Box<[u8]> {
        let mut data: [u8; 4] = [0, 0, 0, 0];
        data[0] = (self.packet_body_length & 0xFF) as u8;
        data[1] = (self.packet_body_length >> 8) as u8;
        data[2] = (self.packet_body_length >> 16) as u8;
        data[3] = self.packet_sequence_number;
        Box::from(data)
    }
}

impl HeaderPacket {
    fn get_packet_sequence_number(&self) -> u8 {
        self.packet_sequence_number
    }
}


struct EOFPacket {
    header: HeaderPacket,
    field_count: u8,
    warning_count: u16,
    status_flag: u16,
}

impl<'a> Packet<'a> for EOFPacket {
    fn from_bytes(&mut self, buf: &'a [u8]) {
        let mut index = 0;
        self.field_count = buf[index];
        index += 1;
        self.warning_count = read_unsigned_short_little_endian(&buf[index..]);
        index += 2;
        self.status_flag = read_unsigned_short_little_endian(&buf[index..])
    }

    fn to_bytes(&mut self) -> Box<[u8]> {
        let mut index = 0;
        let mut data = [0 as u8, 5];
        data[index] = self.field_count;
        index += 1;
        write_unsigned_short_little_endian(self.warning_count, index, &mut data[index..]);
        index += 2;
        write_unsigned_short_little_endian(self.status_flag, index, &mut data[index..]);
        Box::from(data)
    }
}

struct ErrorPacket<'a> {
    header: HeaderPacket,
    field_count: u8,
    error_number: u16,
    sql_state_marker: u8,
    sql_state: &'a [u8],
    message: &'a str,
}


impl<'a> ErrorPacket<'a> {
    fn new() -> ErrorPacket<'a> {
        ErrorPacket {
            header: HeaderPacket { packet_body_length: 0, packet_sequence_number: 0 },
            field_count: 0,
            error_number: 0,
            sql_state_marker: 0,
            sql_state: [0 as u8, 1].borrow(),
            message: "",
        }
    }
}


impl<'a, 'b: 'a> Packet<'b> for ErrorPacket<'a> {
    fn from_bytes(&mut self, buf: &'b [u8]) {
        let mut index = 0;
        self.field_count = buf[0];
        index += 1;
        self.error_number = read_unsigned_short_little_endian(&buf[index..]);
        index += 2;
        self.sql_state_marker = buf[index];
        index += 1;
        self.sql_state = &buf[index..(index + 5)];
        index += 5;
        let s = match from_utf8(&buf[index..]) {
            Ok(v) => v,
            Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
        };
        self.message = s;
    }

    fn to_bytes(&mut self) -> Box<[u8]> {
        todo!()
    }
}

struct FieldPacket<'a> {
    header: HeaderPacket,
    catalog: &'a str,
    db: &'a str,
    table: &'a str,
    original_table: &'a str,
    name: &'a str,
    original_name: &'a str,
    character: u16,
    length: u32,
    type_: u8,
    flags: u16,
    decimals: u8,
    definition: &'a str,
}


impl<'a, 'b : 'a> Packet<'b> for FieldPacket<'a> {
    fn from_bytes(&mut self, buf: &'a [u8]) {
        let mut index = 0;
        let mut reader = LengthCodedStringReader::new(index);
        self.catalog = reader.read_length_coded_string(&buf);
        self.db = reader.read_length_coded_string(&buf);
        self.table = reader.read_length_coded_string(buf);
        self.original_table = reader.read_length_coded_string(buf);
        self.name = reader.read_length_coded_string(buf);
        self.original_name = reader.read_length_coded_string(buf);
        index = reader.index() + 1;
        self.character = read_unsigned_short_little_endian(&buf[index..index + 2]);
        index += 2;
        self.length = read_unsigned_integer_little_endian(&buf[index..index + 4]);
        index += 4;
        self.type_ = buf[index];
        index += 1;
        self.flags = read_unsigned_short_little_endian(&buf[index..index + 2]);
        index += 2;
        self.decimals = buf[index];
        index += 1;
        // skip filter
        index += 2;
        if index < buf.len() {
            reader.set_index(index);
            self.definition = reader.read_length_coded_string(buf);
        }
    }

    fn to_bytes(&mut self) -> Box<[u8]> {
        todo!()
    }
}


struct HandshakeInitializationPacket<'a> {
    header: HeaderPacket,
    protocol_version: u8,
    server_version: &'a str,
    thread_id: u32,
    seed: &'a [u8],
    server_capabilities: u16,
    server_charset_number: u8,
    server_status: u16,
    rest_of_scramble_buff: &'a [u8],
    auth_plugin_name: &'a [u8],
}


impl<'a, 'b: 'a> Packet<'b> for HandshakeInitializationPacket<'a> {
    #[allow(arithmetic_overflow)]
    fn from_bytes(&mut self, buf: &'b [u8]) {
        let mut index = 0;
        self.protocol_version = buf[index];
        index += 1;
        let server_version_bytes = read_null_terminated_bytes(buf);
        self.server_version = from_utf8(server_version_bytes).unwrap();
        index += server_version_bytes.len() + 1;
        self.thread_id = read_unsigned_integer_little_endian(buf);
        index += 4;
        self.seed = &buf[index..index + 8];
        index += 8;
        index += 1; // 1 byte (filler) always 0x00
        self.server_capabilities = read_unsigned_short_little_endian(&buf[index..index + 2]);
        index += 2;
        if buf.len() > index {
            self.server_charset_number = buf[index];
            index += 1;
            self.server_status = read_unsigned_short_little_endian(&buf[index..index + 2]);
            index += 2;
            let capability_flags2 = read_unsigned_short_little_endian(&buf[index..index + 2]);
            let capabilities = ((capability_flags2 << 16) | self.server_capabilities) as i32;
            // int authPluginDataLen = -1;
            // if ((capabilities & Capability.CLIENT_PLUGIN_AUTH) != 0) {
            // authPluginDataLen = data[index];
            // }
            index += 1;
            index += 10;

            if (capabilities & CLIENT_SECURE_CONNECTION) != 0 {
                // int len = Math.max(13, authPluginDataLen - 8);
                // this.authPluginDataPart2 =
                // buffer.readFixedLengthString(len);// scramble2

                // Packet规定最后13个byte是剩下的scrumble,
                // 但实际上最后一个字节是0, 不应该包含在scrumble中.
                self.rest_of_scramble_buff = &buf[index..index + 12];
            }
            index += 12 + 1;

            if (capabilities & CLIENT_PLUGIN_AUTH) != 0 {
                self.auth_plugin_name = read_null_terminated_bytes(buf).borrow()
            }
        }
    }

    fn to_bytes(&mut self) -> Box<[u8]> {
        todo!()
    }
}


struct OKPacket<'a> {
    header: HeaderPacket,
    field_count: u8,
    affected_rows: &'a [u8],
    insert_id: &'a [u8],
    server_status: u16,
    warning_count: u16,
    message: &'a str,
}


impl<'a, 'b: 'a> Packet<'b> for OKPacket<'a> {
    /**
     * <pre>
     *  VERSION 4.1
     *  Bytes                       Name
     *  -----                       ----
     *  1   (Length Coded Binary)   field_count, always = 0
     *  1-9 (Length Coded Binary)   affected_rows
     *  1-9 (Length Coded Binary)   insert_id
     *  2                           server_status
     *  2                           warning_count
     *  n   (until end of packet)   message
     * </pre>
     *
     */
    fn from_bytes(&mut self, buf: &'b [u8]) {
        let mut index = 0;
        self.field_count = buf[0];
        index += 1;
        self.affected_rows = read_binary_coded_length_bytes(buf, index);
        index += self.affected_rows.len();
        self.insert_id = read_binary_coded_length_bytes(buf, index);
        self.server_status = read_unsigned_short_little_endian(&buf[index..index + 2]);
        index += 2;
        self.warning_count = read_unsigned_short_little_endian(&buf[index..index + 2]);
        index += 2;
        self.message = from_utf8(&buf[index..]).unwrap();
    }

    fn to_bytes(&mut self) -> Box<[u8]> {
        todo!()
    }
}


struct Reply323Packet<'a> {
    header: HeaderPacket,
    seed: &'a [u8],
}


impl<'a, 'b: 'a> Packet<'b> for Reply323Packet<'a> {
    fn from_bytes(&mut self, buf: &'b [u8]) {
        todo!()
    }

    fn to_bytes(&mut self) -> Box<[u8]> {
        return if self.seed.len() == 0 {
            Box::from([0 as u8])
        } else {
            let mut out = vec![];
            for index in 0..self.seed.len() {
                out.push(self.seed[index])
            }
            Box::from(out)
        };
    }
}

struct ResultSetHeaderPacket {
    header: HeaderPacket,
    column_count: i64,
    extra: i64,
}

impl<'b> Packet<'b> for ResultSetHeaderPacket {
    fn from_bytes(&mut self, buf: &[u8]) {
        let mut index = 0;
        let column_count_bytes = read_binary_coded_length_bytes(buf, index);
        self.column_count = read_length_coded_binary(column_count_bytes, index);
        index += column_count_bytes.len();
        if index < buf.len() - 1 {
            self.extra = read_length_coded_binary(buf, index);
        }
    }

    fn to_bytes(&mut self) -> Box<[u8]> {
        todo!()
    }
}

struct ResultSetPacket<'a> {
    socket_address: &'a str,
    field_descriptors: Vec<FieldPacket<'a>>,
    field_values: Vec<&'a str>,
}

impl<'a> ResultSetPacket<'a> {
    fn new() -> ResultSetPacket<'a> {
        ResultSetPacket {
            socket_address: "",
            field_descriptors: vec![],
            field_values: vec![],
        }
    }


    pub fn socket_address(&self) -> &'a str {
        self.socket_address
    }
    pub fn field_descriptors(&self) -> &Vec<FieldPacket<'a>> {
        &self.field_descriptors
    }
    pub fn field_values(&self) -> &Vec<&'a str> {
        &self.field_values
    }

    pub fn set_socket_address(&mut self, socket_address: &'a str) {
        self.socket_address = socket_address;
    }
    pub fn set_field_descriptors(&mut self, field_descriptors: Vec<FieldPacket<'a>>) {
        self.field_descriptors = field_descriptors;
    }
    pub fn set_field_values(&mut self, field_values: Vec<&'a str>) {
        self.field_values = field_values;
    }
}


struct RowDataPacket<'a> {
    header: HeaderPacket,
    columns: Vec<&'a str>,
}

impl<'a, 'b: 'a> Packet<'b> for RowDataPacket<'a> {
    fn from_bytes(&mut self, buf: &'b [u8]) {
        let index = 0;
        let mut reader = LengthCodedStringReader::new(index);
        loop {
            self.columns.push(reader.read_length_coded_string(buf));
            if reader.index() >= buf.len() {
                break
            }
        }


    }

    fn to_bytes(&mut self) -> Box<[u8]> {
        todo!()
    }
}

impl <'a>RowDataPacket<'a> {

    pub fn header(&self) -> &HeaderPacket {
        &self.header
    }
    pub fn columns(&self) -> &Vec<&'a str> {
        &self.columns
    }


    pub fn set_header(&mut self, header: HeaderPacket) {
        self.header = header;
    }
    pub fn set_columns(&mut self, columns: Vec<&'a str>) {
        self.columns = columns;
    }
    pub fn new() -> Self {
        Self { header: HeaderPacket{ packet_body_length: 0, packet_sequence_number: 0 }, columns: vec![] }
    }
}

struct LengthCodedStringReader<'a> {
    encoding: &'a str,
    index: usize,
}


impl<'a> LengthCodedStringReader<'a> {
    fn new(index: usize) -> LengthCodedStringReader<'a> {
        LengthCodedStringReader {
            encoding: "",
            index,
        }
    }

    fn read_length_coded_string<'b>(&mut self, buf: &'b [u8]) -> &'b str {
        let bytes = read_binary_coded_length_bytes(buf, self.index);
        let length = read_length_coded_binary(buf, self.index);
        self.set_index(self.index + bytes.len());
        if length == NULL_LENGTH as i64 {
            return "";
        }
        from_utf8(&buf[self.index..(self.index + length as usize)]).unwrap()
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn set_index(&mut self, index: usize) {
        self.index = index;
    }
}


const NULL_LENGTH: i32 = -1;

fn read_none_terminated_bytes(buf: &[u8]) -> &[u8] {
    for (i, b) in buf.iter().enumerate() {
        if *b as i32 == NULL_TERMINATED_STRING_DELIMITER as i32 {
            return &buf[0..i];
        }
    }
    &buf[..]
}

#[allow(arithmetic_overflow)]
fn read_unsigned_short_little_endian(buf: &[u8]) -> u16 {
    ((buf[0] & 0xFF) | ((buf[1] & 0xFF) << 8)) as u16
}

#[allow(arithmetic_overflow)]
fn read_unsigned_short_little_endian_index(buf: &[u8], index: usize) -> u16 {
    ((buf[index] & 0xFF) | ((buf[index + 1] & 0xFF) << 8)) as u16
}

#[allow(arithmetic_overflow)]
fn read_unsigned_integer_little_endian(buf: &[u8]) -> u32 {
    ((buf[0] & 0xFF) as u32 | ((buf[1] & 0xFF) << 8) as u32
        | ((buf[2] & 0xFF) << 16) as u32 | ((buf[3] & 0xFF) << 24) as u32)
}

#[allow(arithmetic_overflow)]
fn read_unsigned_medium_little_endian_index(buf: &[u8], index: usize) -> u32 {
    ((buf[index] & 0xFF) | ((buf[index + 1] & 0xFF) << 8) | ((buf[index + 2] & 0xFF) << 16)) as u32
}

#[allow(arithmetic_overflow)]
fn read_unsigned_long_little_endian_index(buf: &[u8], index: usize) -> u64 {
    let mut accumulation = 0;
    let mut shift_by = 0;
    for index in index..index + 8 {
        accumulation |= ((buf[index] as u64 & 0xff) << shift_by);
        shift_by += 8;
    }
    accumulation as u64
}

fn write_unsigned_short_little_endian(data: u16, mut start: usize, buf: &mut [u8]) {
    buf[start] = (data & 0xFF) as u8;
    start += 1;
    buf[start] = ((data >> 8) & 0xFF) as u8;
}

fn read_binary_coded_length_bytes(buf: &[u8], mut index: usize) -> &[u8] {
    let mark = buf[index] & 0xFF;
    return match mark {
        251 => {
            &buf[index..index]
        }
        252 => {
            &buf[index..index + 2]
        }
        253 => {
            &buf[index..index + 3]
        }
        254 => {
            &buf[index..index + 8]
        }
        _ => {
            &[]
        }
    };
}

fn read_length_coded_binary(buf: &[u8], mut index: usize) -> i64 {
    let mark = buf[index] & 0xFF;
    index += 1;
    match mark {
        251 => {
            return NULL_LENGTH as i64;
        }
        252 => {
            return read_unsigned_short_little_endian_index(buf, index) as i64;
        }
        253 => {
            return read_unsigned_medium_little_endian_index(buf, index) as i64;
        }
        254 => {
            return read_unsigned_long_little_endian_index(buf, index) as i64;
        }
        _ => {}
    }
    return mark as i64;
}

fn read_null_terminated_bytes(buf: &[u8]) -> &[u8] {
    let i = 0;
    for b in buf {
        let item = *b;
        if item == NULL_TERMINATED_STRING_DELIMITER {
            break;
        }
    }
    &buf[0..i]
}