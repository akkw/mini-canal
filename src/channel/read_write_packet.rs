use crate::channel::TcpSocketChannel;
use crate::command::{HeaderPacket, Packet};

pub fn read_header(ch: &mut Box<dyn TcpSocketChannel>) -> Option<HeaderPacket> {
    let mut packet = HeaderPacket::new();
    let mut buf = [0 as u8; 4];
    ch.read(&mut buf).map_or(Option::None, |_f| {
        packet.from_bytes(&buf);
        Option::Some(packet)
    })
}

pub fn read_header_timeout(ch: &mut Box<dyn TcpSocketChannel>, timeout: u32) -> Option<HeaderPacket> {
    let mut packet = HeaderPacket::new();
    let mut buf = [0 as u8; 4];
    ch.read_with_timeout(&mut buf, timeout).map_or(Option::None, |f| {
        packet.from_bytes(&buf);
        Option::Some(packet)
    })
}

pub fn read_bytes(ch:& mut  Box<dyn TcpSocketChannel>, len: i64) ->  Box<[u8]> {
    ch.read_len(len)
}

pub fn write_pkg(ch: &mut Box<dyn TcpSocketChannel>, srcs: &[u8]) {
    ch.write(srcs);
}

pub fn write_body(ch: &mut Box<dyn TcpSocketChannel>, body: &[u8]) {
    write_body0(ch, body, 0)
}

pub fn write_body0(ch: &mut Box<dyn TcpSocketChannel>, srcs: &[u8], packet_seq_number: u8) {
    HeaderPacket::new_para(srcs.len() as i64, packet_seq_number);
    ch.write(srcs);
}



