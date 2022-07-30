use bit_set::BitSet;
use mysql_binlog_parse::instance::metadata::EntryPosition;
use mysql_binlog_parse::instance::running::MysqlEventParser;
use mysql_binlog_parse::parse::support::AuthenticationInfo;
fn main() {
    let info = AuthenticationInfo::form(String::from("127.0.0.1"),
                                        3306,
                                        String::from("root"),
                                        String::from("root"),
                                        String::from("test"));
    let mut parser = MysqlEventParser::from(info);
    let position = Option::Some(EntryPosition::from_position(String::from("mysql-bin.000003"), 4));
    parser.set_position(position);
    parser.start();


}