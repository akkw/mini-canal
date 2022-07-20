use crate::channel::mysql_socket::{MysqlConnector};
use crate::channel::read_write_packet::{read_bytes, read_header, write_body, write_pkg};
use crate::channel::TcpSocketChannel;
use crate::command::client::QueryCommandPacket;
use crate::command::{Packet};
use crate::command::server::{EOFPacket, ErrorPacket, FieldPacket, OKPacket, ResultSetHeaderPacket, ResultSetPacket, RowDataPacket};

pub struct MysqlQueryExecutor<'a> {
    connection: &'a mut MysqlConnector,
}

impl<'a> MysqlQueryExecutor<'a> {
    pub fn from(connection: &'a mut MysqlConnector) -> Self {
        MysqlQueryExecutor {
            connection
        }
    }


    pub fn query(&mut self, sql: &str) -> Result<ResultSetPacket, String> {
        let mut query_command = QueryCommandPacket::from(sql);
        let command_bytes = query_command.to_bytes();


        write_body(&mut self.connection.channel_as_mut().as_mut().unwrap(), &command_bytes);
        let body = read_next_packet(&mut self.connection.channel_as_mut().as_mut().unwrap());
        if body[0] > 128 {
            let mut error = ErrorPacket::new();
            error.from_bytes(&body);
            return Result::Err(format!("{} \nwith error, sql: {}\n", error, sql));
        }

        let mut rs_header = ResultSetHeaderPacket::new();
        rs_header.from_bytes(&body);

        let fields = self.read_columns_name(rs_header);

        read_eof_packet(&mut self.connection.channel_as_mut().as_mut().unwrap()).unwrap();

        let row_data_list = self.read_row_data();

        let mut result_set_packet = ResultSetPacket::new();
        for i in fields.into_iter() {
            result_set_packet.field_descriptors_as_mut().push(i);
        }

        for row_data in row_data_list.into_iter() {
            for column in row_data.columns().into_iter() {
                result_set_packet.field_values_as_mut().push(column);
            }
        }
        Result::Ok(result_set_packet)
    }

    pub fn query_multi(&mut self, sql: &str) -> Result<Vec<ResultSetPacket>, String> {
        let mut query_command = QueryCommandPacket::from(sql);
        let body = query_command.to_bytes();
        write_pkg(&mut self.connection.channel_as_mut().as_mut().unwrap(), &body);


        let mut result_sets = vec![];
        let mut more_result = true;
        while more_result {
            let body = read_next_packet(&mut self.connection.channel_as_mut().as_mut().unwrap());
            if body[0] > 127 {
                let mut error = ErrorPacket::new();
                error.from_bytes(&body);
                return Result::Err(format!("{} \nwith error, sql: {}\n", error, sql));
            }
            let mut rs_handler = ResultSetHeaderPacket::new();
            rs_handler.from_bytes(&body);

            let fields = self.read_columns_name(rs_handler);

            more_result = read_eof_packet(&mut self.connection.channel_as_mut().as_mut().unwrap()).unwrap();

            let row_data_list = self.read_row_data();

            let mut result_set = ResultSetPacket::new();

            for fp in fields.into_iter() {
                result_set.field_descriptors_as_mut().push(fp);
            }

            for row_data in row_data_list.into_iter() {
                for column in row_data.columns().into_iter() {
                    result_set.field_values_as_mut().push(column);
                }
            }

            result_sets.push(result_set)
        }

        Result::Ok(result_sets)
    }

    fn read_columns_name(&mut self, columns: ResultSetHeaderPacket) -> Vec<FieldPacket> {
        let mut fields = vec![];
        let mut body;
        for _i in 0..columns.column_count() {
            let mut fp = FieldPacket::new();
            body = read_next_packet(&mut self.connection.channel_as_mut().as_mut().unwrap());
            fp.from_bytes(&body);
            fields.push(fp);
        }
        fields
    }
    fn read_row_data(&mut self) -> Vec<RowDataPacket> {
        let mut row_data_list = vec![];

        loop {
            let body = read_next_packet(&mut self.connection.channel_as_mut().as_mut().unwrap());
            if body[0] == 254 {
                break;
            }
            let mut row_data = RowDataPacket::new();
            row_data.from_bytes(&body);
            row_data_list.push(row_data);
        }
        row_data_list
    }
}


fn read_next_packet(ch: &mut Box<dyn TcpSocketChannel>) -> Box<[u8]> {
    let header = read_header(ch).unwrap();
    return read_bytes(ch, header.packet_body_length());
}

fn read_eof_packet(ch: &mut Box<dyn TcpSocketChannel>) -> Result<bool, String> {
    let body = read_next_packet(ch);
    let mut eof_packet = EOFPacket::new();
    eof_packet.from_bytes(&body);

    if body[0] != 254 {
        return Result::Err(format!("EOF Packet is expected, but packet with field_count {} is found.", body[0]));
    }

    return Result::Ok((eof_packet.status_flag() & 0x0008) != 0);
}

pub struct MysqlUpdateExecutor<'a> {
    connection: &'a mut MysqlConnector,
}

impl<'a> MysqlUpdateExecutor<'a> {
    pub fn new(connection: &'a mut MysqlConnector) -> Self {
        Self { connection }
    }


    pub fn update(&mut self, sql: &str) -> Result<u32, String> {
        let mut update_packet = QueryCommandPacket::from(sql);
        let update_bytes = update_packet.to_bytes();
        write_body(&mut self.connection.channel_as_mut().as_mut().unwrap(), &update_bytes);

        let header = read_header(&mut self.connection.channel_as_mut().as_mut().unwrap()).unwrap();
        let body = read_bytes(&mut self.connection.channel_as_mut().as_mut().unwrap(), header.packet_body_length());

        if body[0] > 127 {
            let mut error = ErrorPacket::new();
            error.from_bytes(&body);
            return Result::Err(format!("{} \nwith error, sql: {}\n", error, sql));
        }

        let mut ok_packet = OKPacket::new();
        ok_packet.from_bytes(&body);
        Result::Ok(ok_packet.affected_rows()[0] as u32)
    }
}

fn _read_unsigned_integer_little_endian(buf: &[u8]) -> u32 {
    (buf[0] as u8 & 0xFF) as u32 | ((buf[1] as u32 & 0xFF) << 8)
        | ((buf[2] as u32 & 0xFF) << 16) | ((buf[3] as u32 & 0xFF) << 24)
}
