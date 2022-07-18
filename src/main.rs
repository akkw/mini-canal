use std::mem;
use num::ToPrimitive;
use mysql_binlog_parse::command::get_i64;
use mysql_binlog_parse::instance::log_buffer::LogBuffer;

use mysql_binlog_parse::instance::running::MysqlEventParser;
use mysql_binlog_parse::parse::support::AuthenticationInfo;
fn main() {
    let info = AuthenticationInfo::form(String::from("127.0.0.1"),
                                        3306,
                                        String::from("root"),
                                        String::from("root"),
                                        String::from("test"));
    let mut parser = MysqlEventParser::from(info);
    // parser.start();

}