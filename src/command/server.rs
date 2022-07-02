use crate::command::{*};

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