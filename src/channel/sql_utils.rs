use std::borrow::Borrow;
use crate::channel::mysql_socket::MysqlConnection;
use crate::channel::read_write_packet::{read_bytes, read_header, write_pkg};
use crate::channel::TcpSocketChannel;
use crate::command::client::QueryCommandPacket;
use crate::command::{HeaderPacket, Packet};
use crate::command::server::{EOFPacket, ErrorPacket, FieldPacket, ResultSetHeaderPacket, ResultSetPacket, RowDataPacket};

pub struct MysqlQueryExecutor<'a> {
    connection: MysqlConnection<'a>,
}

impl<'a> MysqlQueryExecutor<'a> {
    pub fn new(connection: MysqlConnection<'a>) -> Self {
        Self { connection }
    }


    pub fn query(&mut self, sql: &str) -> Result<ResultSetPacket, String> {
        let mut query_command = QueryCommandPacket::from(sql);
        let command_bytes = query_command.to_bytes();
        write_pkg(&mut self.connection.connector().channel().as_mut().unwrap(), &command_bytes);
        let body = read_next_packet(&mut self.connection.connector().channel().as_mut().unwrap());
        if body[0] < 0 {
            let mut error = ErrorPacket::new();
            error.from_bytes(&body);
            return Result::Err(format!("{} \nwith error, sql: {}\n", error, sql));
        }

        let mut rs_header = ResultSetHeaderPacket::new();
        rs_header.from_bytes(&body);

        let mut fields = vec![];
        let mut body;
        for i in 0..rs_header.column_count() {
            let mut fp = FieldPacket::new();
            body = read_next_packet(&mut self.connection.connector().channel().as_mut().unwrap());
            fp.from_bytes(&body);
            fields.push(fp);
        }
        read_eof_packet(&mut self.connection.connector().channel().as_mut().unwrap());

        let mut row_data_list = vec![];

        loop {
            let body = read_next_packet(&mut self.connection.connector().channel().as_mut().unwrap());
            if body[0] == 254 {
                break
            }
            let row_data = RowDataPacket::new();
            row_data_list.push(row_data);
        }

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