mod channel;
mod command;
mod utils;

use crate::channel::mysql_socket::MysqlConnector;
use crate::channel::TcpSocketChannel;
use crate::channel::TcpChannel;
use crate::utils::mysql_password_encrypted::scramble323;
fn main() {
    let mut connector = MysqlConnector::new_user_password("127.0.0.1", 3306, "root", "root");
    connector.connect();
}