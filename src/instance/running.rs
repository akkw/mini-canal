use crate::channel::mysql_socket::{MysqlConnection};
use crate::instance::metadata::EntryPosition;
use crate::instance::parser::{LogEventConvert};
use crate::parse::support::AuthenticationInfo;

pub struct MysqlEventParser {
    database_info: Option<AuthenticationInfo>,
    master_position: Option<EntryPosition>,
    meta_connection: Option<MysqlConnection>,
    binlog_parser: Option<LogEventConvert>,
    running: bool,
    server_id: i64,
    position: Option<EntryPosition>
}

impl MysqlEventParser {
    pub fn from(authentication_info: AuthenticationInfo) -> MysqlEventParser {
        MysqlEventParser {
            database_info: Option::Some(authentication_info),
            master_position: None,
            meta_connection: None,
            binlog_parser: None,
            running: false,
            server_id: 0,
            position: None
        }
    }

    pub fn start(&mut self) where {
        self.binlog_parser = build_parser();
        self.running = true;
        // while self.running {
        let mut connection = build_connection(self.database_info.as_ref().unwrap().address(),
                                              self.database_info.as_ref().unwrap().port(),
                                              self.database_info.as_ref().unwrap().username(),
                                              self.database_info.as_ref().unwrap().password(),
                                              self.database_info.as_ref().unwrap().default_database_name());
        self.pre_dump(&connection);

        connection.connect();

        let server_id = connection.query_server_id();
        if server_id > 0 {
            self.server_id = server_id;
        }
        let position;
        if let Some(pos) =  &self.position {
            position = pos.clone();
        } else {
            position = self.find_start_position().unwrap();
        }



        connection.reconnect();


        connection.at_dump_sink(position.journal_name().clone().unwrap().as_str(), position.position().unwrap())
        // }
    }


    fn pre_dump(&mut self, connection: &MysqlConnection) {
        self.meta_connection = Option::Some(connection.fork());

        self.meta_connection.as_mut().unwrap().connect();
    }

    fn find_start_position(&mut self) -> Result<EntryPosition, String> {
        let packet = self.meta_connection.as_mut().unwrap().query("show master status");
        let fields = packet.field_values();
        if fields.len() == 0 {
            return Result::Err(String::from("command : 'show master status' has an error! pls check. you need (at least one of) the SUPER,REPLICATION CLIENT privilege(s) for this operation"));
        }
        let journal_name = fields.get(0).unwrap();
        let position = fields.get(1).unwrap().parse::<u32>().unwrap();
        let entry = EntryPosition::from_position(String::from(journal_name), position);
        Result::Ok(entry)
    }
    pub fn set_position(&mut self, position: Option<EntryPosition>) {
        self.position = position;
    }
}


fn build_parser() -> Option<LogEventConvert> {
    Option::Some(LogEventConvert {})
}

fn build_connection(address: &str, port: u16, username: &str, password: &str, schema: &str) -> MysqlConnection {
    let mut connection = MysqlConnection::from_schema(
        String::from(address),
        port,
        String::from(username),
        String::from(password),
        String::from(schema),
    );
    connection.set_slave_id(1000);
    connection
}

