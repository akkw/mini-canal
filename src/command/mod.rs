use std::borrow::Borrow;

pub mod packet_utils {}

trait Packet<'a> {
    fn fromBytes(&mut self, buf: &'a [u8]);
    fn toBytes(&mut self, buf: &[u8]) -> &[u8];
}


struct AuthSwitchRequestMoreData<'a> {
    command: i32,
    status: i32,
    authData: &'a [u8],
}


impl<'a, 'b: 'a> Packet<'b> for AuthSwitchRequestMoreData<'a> {

    fn fromBytes(&mut self, buf: &'b [u8]) {
        let mut index = 0;
        self.status = buf[index] as i32;
        index += 1;
        self.authData = read_None_terminated_bytes(&buf, index);
    }

    fn toBytes(&mut self, buf: &[u8]) -> &[u8] {
        todo!()
    }
}

struct AuthSwitchRequestPacket<'a> {
    command: i32,
    authName: &'a str,
    authData: &'a [u8],
}


impl <'a, 'b: 'a>Packet<'b> for AuthSwitchRequestPacket<'a> {
    fn fromBytes(&mut self, buf: &'b [u8]) {
        let mut index = 0;
        self.command = buf[index] as i32;
        index+=1;
        let authName = read_None_terminated_bytes(buf, index);
        let s = match std::str::from_utf8(authName) {
            Ok(v) => v,
            Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
        };
        self.authName =  s;
    }

    fn toBytes(&mut self, buf: &[u8]) -> &[u8] {
        todo!()
    }
}


const NULL_TERMINATED_STRING_DELIMITER: i32 = 0x00;

fn read_None_terminated_bytes(buf: &[u8], index: usize) -> &[u8] {
    for (i, b) in buf.iter().enumerate() {
        if *b as i32 == NULL_TERMINATED_STRING_DELIMITER {
            return &buf[0..i];
        }
    }
    &buf[..]
}