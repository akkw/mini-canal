mod channel;

use crate::channel::SocketChannel;
use crate::channel::TcpChannel;
fn main() {
    let mut channel = TcpChannel::new("127.0.0.1", 50001);
    let mut buf = [0u8; 10];
    channel.read(&mut buf);
    println!("{}",String::from_utf8_lossy(&buf).to_string());
    let size = channel.write("xie shu ju".as_bytes());
    println!("{}",size.unwrap())
}