
use crate::channel::TcpSocketChannel;
use crate::command::{HeaderPacket, Packet};

fn read_header(mut ch: Box<dyn TcpSocketChannel>) -> Option<HeaderPacket> {
    let mut packet = HeaderPacket::new();
    let mut buf = [0 as u8; 4];
    ch.read(&mut buf).map_or(Option::None, |f| {
        packet.from_bytes(&buf);
        Option::Some(packet)
    })
}

fn read_header_timeout(mut ch: Box<dyn TcpSocketChannel>, timeout: usize) -> Option<HeaderPacket> {
    let mut packet = HeaderPacket::new();
    let mut buf = [0 as u8; 4];
    ch.read_with_timeout(&mut buf, timeout).map_or(Option::None, |f| {
        packet.from_bytes(&buf);
        Option::Some(packet)
    })
}

fn read_bytes(mut ch: Box<dyn TcpSocketChannel>, len: usize) -> Box<[u8]> {
    ch.read_len(len)
}

fn write_pkg(mut ch: Box<dyn TcpSocketChannel>, srcs: &[u8]) {
    ch.write(srcs);
}

fn write_body(mut ch: Box<dyn TcpSocketChannel>, body: &[u8]) {
    write_body0(ch,body, 0)
}

fn write_body0(mut ch: Box<dyn TcpSocketChannel>, srcs: &[u8], packet_seq_number: u8) {
    HeaderPacket::new_para(srcs.len() as i32, packet_seq_number);
    ch.write(srcs,);
}



