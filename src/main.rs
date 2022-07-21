use bit_set::BitSet;
use mysql_binlog_parse::instance::running::MysqlEventParser;
use mysql_binlog_parse::parse::support::AuthenticationInfo;
fn main() {
    let info = AuthenticationInfo::form(String::from("127.0.0.1"),
                                        3306,
                                        String::from("root"),
                                        String::from("root"),
                                        String::from("test"));
    let mut _parser = MysqlEventParser::from(info);
}