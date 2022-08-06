use std::borrow::Borrow;
use std::str::{*};
use msc::{*};
use capability::{*};

pub mod server;

pub mod client;

pub mod types;

pub mod msc {
    pub const DEFAULT_PROTOCOL_VERSION: u8 = 0x0a;
    pub const NULL_TERMINATED_STRING_DELIMITER: u8 = 0x00;
    pub const MAX_PACKET_LENGTH: u32 = 1 << 24;
    pub const _HEADER_PACKET_LENGTH_FIELD_LENGTH: i32 = 3;
    pub const _HEADER_PACKET_LENGTH_FIELD_OFFSET: i32 = 0;
    pub const _HEADER_PACKET_LENGTH: i32 = 4;
    pub const _HEADER_PACKET_NUMBER_FIELD_LENGTH: i32 = 1;
    pub const _FIELD_COUNT_FIELD_LENGTH: i32 = 1;
    pub const _EVENT_TYPE_OFFSET: i32 = 4;
    pub const _EVENT_LEN_OFFSET: i32 = 9;
    pub const _DEFAULT_BINLOG_FILE_START_POSITION: i64 = 4;
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

pub trait Packet<'a> {
    fn from_bytes(&mut self, buf: &'a [u8]);
    fn to_bytes(&mut self) -> Box<[u8]>;
}



/**
 * <pre>
 * Offset  Length     Description
 *   0       3        Packet body length stored with the low byte first.
 *   3       1        Packet sequence number. The sequence numbers are reset with each new command.
 *                      While the correct packet sequencing is ensured by the underlying transmission protocol,
 *                      this field is used for the sanity checks of the application logic.
 * </pre>
 *
 * <br>
 * The Packet Header will not be shown in the descriptions of packets that
 * follow this section. Think of it as always there. But logically, it
 * "precedes the packet" rather than "is included in the packet".<br>
 *
 */
pub struct HeaderPacket {
    /**
     * this field indicates the packet length that follows the header, with
     * header packet's 4 bytes excluded.
     */
    packet_body_length: i64,
    packet_sequence_number: u8,
}

impl HeaderPacket {
    pub fn packet_body_length(&self) -> i64 {
        self.packet_body_length
    }
    pub fn packet_sequence_number(&self) -> u8 {
        self.packet_sequence_number
    }

    pub fn set_packet_body_length(&mut self, packet_body_length: i64) {
        self.packet_body_length = packet_body_length;
    }
    pub fn set_packet_sequence_number(&mut self, packet_sequence_number: u8) {
        self.packet_sequence_number = packet_sequence_number;
    }
    pub fn new() -> Self {
        Self { packet_body_length: 0, packet_sequence_number: 0 }
    }
    pub fn new_para(packet_body_length: i64, packet_sequence_number: u8) -> Self {
        Self { packet_body_length, packet_sequence_number }
    }
}

impl<'a> Packet<'a> for HeaderPacket {
    #[allow(arithmetic_overflow)]
    fn from_bytes(&mut self, buf: &'a [u8]) {
        self.packet_body_length = (buf[0] & 0xFF) as i64 | (((buf[1] & 0xFF) as i64) << 8) as i64 | (((buf[2] & 0xFF) as i64) << 16) as i64;
        self.set_packet_sequence_number(buf[3]);
    }

    fn to_bytes(&mut self) -> Box<[u8]> {
        let mut data = [0 as u8; 4];
        data[0] = (self.packet_body_length & 0xFF) as u8;
        data[1] = (self.packet_body_length >> 8) as u8;
        data[2] = (self.packet_body_length >> 16) as u8;
        data[3] = self.packet_sequence_number();
        Box::from(data)
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
    ((buf[0] as u16 & 0xFF) | ((buf[1] as u16 & 0xFF) << 8)) as u16
}

#[allow(arithmetic_overflow)]
fn read_unsigned_short_little_endian_index(buf: &[u8], index: usize) -> u16 {
    ((buf[index] & 0xFF) | (((buf[index + 1] & 0xFF) as u8) << 8)) as u16
}

#[allow(arithmetic_overflow)]
fn read_unsigned_integer_little_endian(buf: &[u8]) -> u32 {
    (buf[0] as u8 & 0xFF) as u32 | ((buf[1] as u32 & 0xFF) << 8)
        | ((buf[2] as u32 & 0xFF) << 16) | ((buf[3] as u32 & 0xFF) << 24)
}

#[allow(arithmetic_overflow)]
fn read_unsigned_medium_little_endian_index(buf: &[u8], index: usize) -> u32 {
    ((buf[index] as u32 & 0xFF) | ((buf[index + 1] as u32 & 0xFF) << 8) | ((buf[index + 2] as u32 & 0xFF) << 16)) as u32
}

#[allow(arithmetic_overflow)]
fn read_unsigned_long_little_endian_index(buf: &[u8], index: usize) -> u64 {
    let mut accumulation = 0;
    let mut shift_by = 0;
    for index in index..index + 8 {
        accumulation |= ((buf[index] & 0xff) as u64) << shift_by;
        shift_by += 8;
    }
    accumulation as u64
}

fn write_unsigned_short_little_endian(data: u16, mut start: usize, buf: &mut [u8]) {
    buf[start] = (data & 0xFF) as u8;
    start += 1;
    buf[start] = ((data >> 8) & 0xFF) as u8;
}

fn read_binary_coded_length_bytes(buf: &[u8], index: usize) -> &[u8] {
    let mark = buf[index] & 0xFF;
    return match mark {
        251 => {
            &buf[index..index + 1]
        }
        252 => {
            &buf[index..index + 3]
        }
        253 => {
            &buf[index..index + 4]
        }
        254 => {
            &buf[index..index + 9]
        }
        _ => {
            &buf[index..index + 1]
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
    let mut size = 0;
    for i in 0..buf.len() {
        if buf[i] == NULL_TERMINATED_STRING_DELIMITER {
            break;
        }

        size += 1;
    }
    &buf[0..size]
}

pub fn write_header_and_body(header: &[u8], body: &[u8]) -> Box<[u8]> {
    let mut out = vec![];
    for i in 0..header.len() {
        out.push(header[i]);
    }
    for i in 0..body.len() {
        out.push(body[i]);
    }
    Box::from(out)
}

pub fn write_unsigned_8byte_little_endian(src: u64, out: &mut Vec<u8>) {
    out.push((src & 0xFF) as u8);
    out.push((src >> 8) as u8);
    out.push((src >> 16) as u8);
    out.push((src >> 24) as u8);
    out.push((src >> 32) as u8);
    out.push((src >> 40) as u8);
    out.push((src >> 48) as u8);
    out.push((src >> 56) as u8);
}

pub fn write_unsigned_4byte_little_endian(src: u32, out: &mut Vec<u8>) {
    out.push((src & 0xFF) as u8);
    out.push((src >> 8) as u8);
    out.push((src >> 16) as u8);
    out.push((src >> 24) as u8);
}

pub fn write_unsigned_2byte_little_endian_vec(src: u16, out: &mut Vec<u8>) {
    out.push((src & 0xFF) as u8);
    out.push((src >> 8) as u8);
}

pub fn write_unsigned_medium_little_endian(src: u32, out: &mut Vec<u8>) {
    out.push((src & 0xFF) as u8);
    out.push((src >> 8) as u8);
    out.push((src >> 16) as u8);
}

pub fn write_null_terminated_string(src: &str, out: &mut Vec<u8>) {
    let bytes = src.as_bytes();
    for i in 0..bytes.len() {
        out.push(bytes[i]);
    }
    out.push(msc::NULL_TERMINATED_STRING_DELIMITER)
}

pub fn write_null_terminated(src: &[u8], out: &mut Vec<u8>) {
    for i in 0..src.len() {
        out.push(src[i]);
    }
    out.push(msc::NULL_TERMINATED_STRING_DELIMITER)
}


fn write_binary_coded_length_bytes(src: &[u8], out: &mut Vec<u8>) {
    // 1. write length byte/bytes
    if src.len() < 252 {
        out.push(src.len() as u8);
    } else if src.len() < (1 << 16) {
        out.push(252);
        write_unsigned_2byte_little_endian_vec(src.len() as u16, out);
    } else if src.len() < (1 << 24) {
        out.push(253);
        write_unsigned_medium_little_endian(src.len() as u32, out);
    } else {
        out.push(254);
        write_unsigned_4byte_little_endian(src.len() as u32, out);
    }
    // 2. write real data followed length byte/bytes
    for i in 0..src.len() {
        out.push(src[i])
    }
}

fn write_fixed_length_bytes_from_start(data: &[u8], len: usize, out: &mut Vec<u8>) {
    write_fixed_length_bytes(data, 0, len, out)
}

fn write_fixed_length_bytes(data: &[u8], being: usize, len: usize, out: &mut Vec<u8>) {
    for i in being..len {
        out.push(data[i])
    }
}
// int ans=0;
// for(int i=0;i<4;i++){
//      ans<<=8;//左移 8 位
//      ans|=a[3-i];//保存 byte 值到 ans 的最低 8 位上
//      intPrint(ans);
// }

pub fn get_i64(bytes: &[u8]) -> i64 {
    ((bytes[0] as i64) << 56) |
        (bytes[1] as i64 & 0xff) << 48 |
        (bytes[2] as i64 & 0xff) << 40 |
        (bytes[3] as i64 & 0xff) << 32 |
        (bytes[4] as i64 & 0xff) << 24 |
        (bytes[5] as i64 & 0xff) << 16 |
        (bytes[6] as i64 & 0xff) << 8 |
        (bytes[7] as i64 & 0xff)
}

