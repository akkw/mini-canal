use std::borrow::Borrow;
use crate::channel::mysql_socket::MysqlConnection;
use crate::channel::read_write_packet::{read_header, write_pkg};
use crate::channel::TcpSocketChannel;
use crate::command::client::QueryCommandPacket;
use crate::command::{HeaderPacket, Packet};
use crate::command::server::ResultSetPacket;

pub struct MysqlQueryExecutor<'a> {
    connection: MysqlConnection<'a>,
}

impl <'a>MysqlQueryExecutor<'a> {
    pub fn new(connection: MysqlConnection<'a>) -> Self {
        Self { connection }
    }


    pub fn query(&mut self,sql: &str) -> ResultSetPacket {
        let mut query_command = QueryCommandPacket::from(sql);
        let command_bytes = query_command.to_bytes();
        write_pkg(&mut self.connection.connector().channel().as_mut().unwrap(), &command_bytes);
        let more_result = true;
        while more_result {
            // 明天继续
        }
        ResultSetPacket::new()
    }
}


fn read_next_packet(ch: &mut Box<dyn TcpSocketChannel>) -> HeaderPacket{
   return read_header(ch).unwrap();
}