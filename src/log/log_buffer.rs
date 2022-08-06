use std::mem;
use std::str::{from_boxed_utf8_unchecked, FromStr};
use bigdecimal::BigDecimal;
use bit_set::BitSet;
use chrono::format::Numeric::Timestamp;
use chrono::{DateTime, NaiveDateTime, Utc};
use encoding::{DecoderTrap, Encoding};
use encoding::all::ISO_8859_1;
use substring::Substring;
// use encoding::DecoderTrap::Strict;
use crate::channel::TcpSocketChannel;
use crate::command::types::Types;
use crate::log::event::{Event, LogEvent, Serializable};
use crate::protocol::mini_canal_entry::Type;
use crate::protocol::mini_canal_packet::Dump_oneof_timestamp_present::timestamp;
use crate::StringResult;
use crate::utils::time::timestamp_to_time;

const NULL_LENGTH: i64 = -1;
const DIG_PER_DEC1: i32 = 9;
const DIG_BASE: i32 = 1000000000;
const DIG_MAX: i32 = DIG_BASE - 1;
const DIG2BYTES: [usize; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
const POWERS10: [usize; 10] = [1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000];
const DIG_PER_INT32: usize = 9;
const SIZE_OF_INT32: usize = 4;

#[derive(Debug)]
pub struct LogBuffer {
    buffer: Vec<u8>,
    origin: usize,
    limit: usize,
    position: usize,
    seminal: u8,
}

impl LogBuffer {
    pub fn from(buffer: Vec<u8>, origin: usize, limit: usize) -> Result<LogBuffer, String> {
        if origin + limit > buffer.len().try_into().unwrap() {
            return Result::Err(String::from(format!("capacity excceed: {}", origin + limit)));
        }
        Result::Ok(LogBuffer {
            buffer,
            origin,
            limit,
            position: origin,
            seminal: 0,
        })
    }

    pub fn duplicate_pos_len(&self, pos: usize, len: usize) -> Result<LogBuffer, String> {
        if pos + len > self.limit {
            return Result::Err(String::from(format!("limit excceed: {}", pos + len)));
        }
        let off = self.origin + pos;

        Result::Ok(LogBuffer {
            buffer: copy_of_range(&self.buffer, off, len),
            origin: 0,
            limit: len,
            position: 0,
            seminal: 0,
        })
    }

    pub fn duplicate_len(&mut self, len: usize) -> Result<LogBuffer, String> {
        if self.position + len > self.origin + self.limit {
            let position = self.position;
            let origin = self.origin;
            return Result::Err(String::from(format!("limit excceed: {}", position + len - origin)));
        }

        let end = self.position + len;
        self.position = end;

        Result::Ok(LogBuffer {
            buffer: copy_of_range(&self.buffer, self.position, end),
            origin: 0,
            limit: len,
            position: 0,
            seminal: 0,
        })
    }

    pub fn duplicate(&self) -> LogBuffer {
        LogBuffer {
            buffer: copy_of_range(&self.buffer, self.origin, self.origin + self.limit),
            origin: 0,
            limit: self.limit,
            position: 0,
            seminal: 0,
        }
    }

    pub fn position(&mut self) -> usize {
        self.position - self.origin
    }


    pub fn up_position(&mut self, new_position: usize) -> Result<bool, String> {
        if new_position > self.limit {
            return Result::Err(String::from(format!("limit excceed: {}", new_position)));
        }
        self.position = self.origin + new_position;
        Result::Ok(true)
    }

    pub fn forward(&mut self, len: usize) -> Result<&mut LogBuffer, String> {
        if self.position + len > self.origin + self.limit {
            let position = self.position;
            let origin = self.origin;
            return Result::Err(String::from(format!("limit excceed: {}", position + len - origin)));
        }

        self.position += len;
        Result::Ok(self)
    }

    pub fn consume(&mut self, len: usize) -> Result<bool, String> {
        if self.limit > len {
            self.limit -= len;
            self.origin += len;
            self.position = self.origin;
            return Result::Ok(true);
        } else if self.limit == len {
            self.limit = 0;
            self.origin = 0;
            self.position = 0;
            return Result::Ok(true);
        } else {
            return Result::Err(String::from(format!("limit excceed: {}", len)));
        }
    }

    pub fn rewind(&mut self) {
        self.position = self.origin;
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn new_limit(&mut self, new_limit: usize) -> Result<bool, String> {
        if self.origin + new_limit > self.buffer.len() {
            let origin = self.origin;
            return Result::Err(String::from(format!("capacity excceed: {}", origin + new_limit)));
        }
        self.limit = new_limit;
        return Result::Ok(true);
    }

    pub fn remaining(&self) -> i64 {
        return (self.limit + self.origin - self.position) as i64;
    }

    pub fn has_remaining(&self) -> bool {
        self.position < self.limit + self.origin
    }

    pub fn get_int8_pos(&self, pos: usize) -> Result<i8, String> {
        if self.check_pos_ge(pos, 0) {
            return Result::Ok(self.buffer[self.origin + pos] as i8);
        }
        return Result::Err(String::from(format!("capacity excceed: {}", pos)));
    }

    pub fn get_int8(&mut self) -> Result<i8, String> {
        if self.check_ge(0) {
            let u8 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok(u8 as i8);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }

    pub fn get_uint8_pos(&self, pos: usize) -> Result<u8, String> {
        if self.check_pos_ge(pos, 0) {
            return Result::Ok(self.buffer[self.origin + pos]);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }


    pub fn get_uint8(&mut self) -> Result<u8, String> {
        if self.check_ge(0) {
            let u8 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok(u8);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }

    #[allow(arithmetic_overflow)]
    pub fn get_int16_pos(&mut self, pos: usize) -> Result<i16, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 1) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            return Result::Ok((i as i16 | (i1 as i16) << 8) as i16);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_int16(&mut self) -> Result<i16, String> {
        if self.check_ge(1) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i as i16 | (i1 as i16) << 8) as i16);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_uint16_pos(&mut self, pos: usize) -> Result<u16, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 1) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            return Result::Ok((i as u16 | (i1 as u16) << 8) as u16);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_uint16(&mut self) -> Result<u16, String> {
        if self.check_ge(1) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i as u16 | (i1 as u16) << 8) as u16);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_int16_pos_big_endian(&self, pos: usize) -> Result<i16, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 1) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            return Result::Ok((i1 as i16 | (i as i16) << 8) as i16);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_int16_big_endian(&mut self) -> Result<i16, String> {
        if self.check_ge(1) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i1 as i16 | (i as i16) << 8) as i16);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }

    #[allow(arithmetic_overflow)]
    pub fn get_uint16_pos_big_endian(&self, pos: usize) -> Result<u16, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 1) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            return Result::Ok((i1 as u16 | (i as u16) << 8) as u16);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_uint16_big_endian(&mut self) -> Result<u16, String> {
        if self.check_ge(1) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i1 as u16 | (i as u16) << 8) as u16);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }


    #[allow(arithmetic_overflow)]
    pub fn get_int24_pos(&mut self, pos: usize) -> Result<i32, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 2) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            return Result::Ok((i as i32 | (i1 as i32) << 8 | (i2 as i32) << 16) as i32);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_int24(&mut self) -> Result<i32, String> {
        if self.check_ge(2) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i as i32 | (i1 as i32) << 8 | (i2 as i32) << 16) as i32);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_uint24_pos(&mut self, pos: usize) -> Result<u32, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 2) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            return Result::Ok((i as u32 | (i1 as u32) << 8 | (i2 as u32) << 16) as u32);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_uint24(&mut self) -> Result<u32, String> {
        if self.check_ge(2) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i as u32 | (i1 as u32) << 8 | (i2 as u32) << 16) as u32);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_int24_pos_big_endian(&self, pos: usize) -> Result<i32, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 2) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            return Result::Ok((i2 as i32 | (i1 as i32) << 8 | (i as i32) << 16) as i32);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_int24_big_endian(&mut self) -> Result<i32, String> {
        if self.check_ge(2) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i2 as i32 | (i1 as i32) << 8 | (i as i32) << 16) as i32);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }

    #[allow(arithmetic_overflow)]
    pub fn get_uint24_pos_big_endian(&self, pos: usize) -> Result<u32, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 2) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            return Result::Ok((i2 as u32 | (i1 as u32) << 8 | (i as u32) << 16) as u32);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_uint24_big_endian(&mut self) -> Result<u32, String> {
        if self.check_ge(2) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i2 as u32 | (i1 as u32) << 8 | (i as u32) << 16) as u32);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }


    #[allow(arithmetic_overflow)]
    pub fn get_int32_pos(&self, pos: usize) -> Result<i32, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 3) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            let i3 = self.buffer[position + 3];
            return Result::Ok((i as i32 | (i1 as i32) << 8 | (i2 as i32) << 16 | (i3 as i32) << 24) as i32);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_int32(&mut self) -> Result<i32, String> {
        if self.check_ge(3) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            let i3 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i as i32 | (i1 as i32) << 8 | (i2 as i32) << 16 | (i3 as i32) << 24) as i32);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_uint32_pos(&mut self, pos: usize) -> Result<u32, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 3) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            let i3 = self.buffer[position + 3];
            return Result::Ok((i as u32 | (i1 as u32) << 8 | (i2 as u32) << 16 | (i3 as u32) << 24) as u32);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_uint32(&mut self) -> Result<u32, String> {
        if self.check_ge(3) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            let i3 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i as u32 | (i1 as u32) << 8 | (i2 as u32) << 16 | (i3 as u32) << 24) as u32);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_int32_pos_big_endian(&self, pos: usize) -> Result<i32, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 3) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            let i3 = self.buffer[position + 3];
            return Result::Ok(((i3 as i32) | (i2 as i32) << 8 | (i1 as i32) << 16 | (i as i32) << 24) as i32);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_int32_big_endian(&mut self) -> Result<i32, String> {
        if self.check_ge(3) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            let i3 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok(((i3 as i32) | (i2 as i32) << 8 | (i1 as i32) << 16 | (i as i32) << 24) as i32);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }

    #[allow(arithmetic_overflow)]
    pub fn get_uint32_pos_big_endian(&self, pos: usize) -> Result<u32, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 3) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            let i3 = self.buffer[position + 3];
            return Result::Ok(((i3 as u32) | (i2 as u32) << 8 | (i1 as u32) << 16 | (i as u32) << 24) as u32);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_uint32_big_endian(&mut self) -> Result<u32, String> {
        if self.check_ge(3) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            let i3 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok(((i3 as u32) | (i2 as u32) << 8 | (i1 as u32) << 16 | (i as u32) << 24) as u32);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }


    #[allow(arithmetic_overflow)]
    pub fn get_int40_pos(&mut self, pos: usize) -> Result<i64, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 4) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            let i3 = self.buffer[position + 3];
            let i4 = self.buffer[position + 4];
            return Result::Ok((i as i64 | (i1 as i64) << 8 | (i2 as i64) << 16 | (i3 as i64) << 24
                | (i4 as i64) << 32) as i64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_int40(&mut self) -> Result<i64, String> {
        if self.check_ge(4) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            let i3 = self.buffer[self.position];
            self.position += 1;
            let i4 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i as i64 | (i1 as i64) << 8 | (i2 as i64) << 16 | (i3 as i64) << 24
                | (i4 as i64) << 32) as i64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_uint40_pos(&mut self, pos: usize) -> Result<u64, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 4) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            let i3 = self.buffer[position + 3];
            let i4 = self.buffer[position + 4];
            return Result::Ok((i as u64 | (i1 as u64) << 8 | (i2 as u64) << 16 | (i3 as u64) << 24
                | (i4 as u64) << 32) as u64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_uint40(&mut self) -> Result<u64, String> {
        if self.check_ge(4) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            let i3 = self.buffer[self.position];
            self.position += 1;
            let i4 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i as u64 | (i1 as u64) << 8 | (i2 as u64) << 16 | (i3 as u64) << 24
                | (i4 as u64) << 32) as u64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_int40_pos_big_endian(&self, pos: usize) -> Result<i64, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 4) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            let i3 = self.buffer[position + 3];
            let i4 = self.buffer[position + 4];
            return Result::Ok((i4 as i64 | (i3 as i64) << 8 | (i2 as i64) << 16 | (i1 as i64) << 24
                | (i as i64) << 32) as i64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_int40_big_endian(&mut self) -> Result<i64, String> {
        if self.check_ge(4) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            let i3 = self.buffer[self.position];
            self.position += 1;
            let i4 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i4 as i64 | (i3 as i64) << 8 | (i2 as i64) << 16 | (i1 as i64) << 24
                | (i as i64) << 32) as i64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }

    #[allow(arithmetic_overflow)]
    pub fn get_uint40_pos_big_endian(&self, pos: usize) -> Result<u64, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 4) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            let i3 = self.buffer[position + 3];
            let i4 = self.buffer[position + 4];
            return Result::Ok((i4 as u64 | (i3 as u64) << 8 | (i2 as u64) < 16 | (i1 as u64) << 24
                | (i as u64) << 32) as u64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_uint40_big_endian(&mut self) -> Result<u64, String> {
        if self.check_ge(4) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            let i3 = self.buffer[self.position];
            self.position += 1;
            let i4 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i4 as u64 | (i3 as u64) << 8 | (i2 as u64) < 16 | (i1 as u64) << 24
                | (i as u64) << 32) as u64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }


    #[allow(arithmetic_overflow)]
    pub fn get_int48_pos(&mut self, pos: usize) -> Result<i64, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 5) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            let i3 = self.buffer[position + 3];
            let i4 = self.buffer[position + 4];
            let i5 = self.buffer[position + 5];
            return Result::Ok((i as i64 | (i1 as i64) << 8 | (i2 as i64) << 16 | (i3 as i64) << 24
                | (i4 as i64) << 32 | (i5 as i64) << 40) as i64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_int48(&mut self) -> Result<i64, String> {
        if self.check_ge(5) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            let i3 = self.buffer[self.position];
            self.position += 1;
            let i4 = self.buffer[self.position];
            self.position += 1;
            let i5 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i as i64 | (i1 as i64) << 8 | (i2 as i64) << 16 | (i3 as i64) << 24
                | (i4 as i64) << 32 | (i5 as i64) << 40) as i64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_uint48_pos(&mut self, pos: usize) -> Result<u64, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 5) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            let i3 = self.buffer[position + 3];
            let i4 = self.buffer[position + 4];
            let i5 = self.buffer[position + 5];
            return Result::Ok((i as u64 | (i1 as u64) << 8 | (i2 as u64) << 16 | (i3 as u64) << 24
                | (i4 as u64) << 32 | (i5 as u64) << 40) as u64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_uint48(&mut self) -> Result<u64, String> {
        if self.check_ge(5) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            let i3 = self.buffer[self.position];
            self.position += 1;
            let i4 = self.buffer[self.position];
            self.position += 1;
            let i5 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i as u64 | (i1 as u64) << 8 | (i2 as u64) << 16 | (i3 as u64) << 24
                | (i4 as u64) << 32 | (i5 as u64) << 40) as u64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_int48_pos_big_endian(&self, pos: usize) -> Result<i64, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 5) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            let i3 = self.buffer[position + 3];
            let i4 = self.buffer[position + 4];
            let i5 = self.buffer[position + 5];
            return Result::Ok((i5 as i64 | (i4 as i64) << 8 | (i3 as i64) << 16 | (i2 as i64) < 24
                | (i1 as i64) << 32 | (i as i64) << 40) as i64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_int48_big_endian(&mut self) -> Result<i64, String> {
        if self.check_ge(5) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            let i3 = self.buffer[self.position];
            self.position += 1;
            let i4 = self.buffer[self.position];
            self.position += 1;
            let i5 = self.buffer[self.position];
            self.position += 1;


            return Result::Ok((i5 as i64 | (i4 as i64) << 8 | (i3 as i64) << 16 | (i2 as i64) < 24
                | (i1 as i64) << 32 | (i as i64) << 40) as i64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }

    #[allow(arithmetic_overflow)]
    pub fn get_uint48_pos_big_endian(&self, pos: usize) -> Result<u64, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 5) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            let i3 = self.buffer[position + 3];
            let i4 = self.buffer[position + 4];
            let i5 = self.buffer[position + 5];
            return Result::Ok((i5 as u64 | (i4 as u64) << 8 | (i3 as u64) << 16 | (i2 as u64) < 24 | (i1 as u64) << 32 | (i as u64) << 40) as u64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_uint48_big_endian(&mut self) -> Result<u64, String> {
        if self.check_ge(5) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            let i3 = self.buffer[self.position];
            self.position += 1;
            let i4 = self.buffer[self.position];
            self.position += 1;
            let i5 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i5 as u64 | (i4 as u64) << 8 | (i3 as u64) << 16 | (i2 as u64) < 24 | (i1 as u64) << 32 | (i as u64) << 40) as u64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }


    #[allow(arithmetic_overflow)]
    pub fn get_int56_pos(&mut self, pos: usize) -> Result<i64, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 6) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            let i3 = self.buffer[position + 3];
            let i4 = self.buffer[position + 4];
            let i5 = self.buffer[position + 5];
            let i6 = self.buffer[position + 6];
            return Result::Ok((i as i64 | (i1 as i64) << 8 | (i2 as i64) << 16 | (i3 as i64) << 24
                | (i4 as i64) << 32 | (i5 as i64) << 40 | (i6 as i64) << 48) as i64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_int56(&mut self) -> Result<i64, String> {
        if self.check_ge(6) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            let i3 = self.buffer[self.position];
            self.position += 1;
            let i4 = self.buffer[self.position];
            self.position += 1;
            let i5 = self.buffer[self.position];
            self.position += 1;
            let i6 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i as i64 | (i1 as i64) << 8 | (i2 as i64) << 16 | (i3 as i64) << 24
                | (i4 as i64) << 32 | (i5 as i64) << 40 | (i6 as i64) << 48) as i64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_uint56_pos(&mut self, pos: usize) -> Result<u64, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 6) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            let i3 = self.buffer[position + 3];
            let i4 = self.buffer[position + 4];
            let i5 = self.buffer[position + 5];
            let i6 = self.buffer[position + 6];
            return Result::Ok((i as u64 | (i1 as u64) << 8 | (i2 as u64) << 16 | (i3 as u64) << 24
                | (i4 as u64) << 32 | (i5 as u64) << 40 | (i6 as u64) << 48) as u64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_uint56(&mut self) -> Result<u64, String> {
        if self.check_ge(6) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            let i3 = self.buffer[self.position];
            self.position += 1;
            let i4 = self.buffer[self.position];
            self.position += 1;
            let i5 = self.buffer[self.position];
            self.position += 1;
            let i6 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i as u64 | (i1 as u64) << 8 | (i2 as u64) << 16 | (i3 as u64) << 24
                | (i4 as u64) << 32 | (i5 as u64) << 40 | (i6 as u64) << 48) as u64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_int56_pos_big_endian(&self, pos: usize) -> Result<i64, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 6) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            let i3 = self.buffer[position + 3];
            let i4 = self.buffer[position + 4];
            let i5 = self.buffer[position + 5];
            let i6 = self.buffer[position + 6];
            return Result::Ok((i6 as i64 | (i5 as i64) << 8 | (i4 as i64) << 16
                | (i3 as i64) << 24 | (i2 as i64) < 32 | (i1 as i64) << 40 | (i as i64) << 48)
                as i64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_int56_big_endian(&mut self) -> Result<i64, String> {
        if self.check_ge(6) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            let i3 = self.buffer[self.position];
            self.position += 1;
            let i4 = self.buffer[self.position];
            self.position += 1;
            let i5 = self.buffer[self.position];
            self.position += 1;
            let i6 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i6 as i64 | (i5 as i64) << 8 | (i4 as i64) << 16
                | (i3 as i64) << 24 | (i2 as i64) < 32 | (i1 as i64) << 40 | (i as i64) << 48)
                as i64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }

    #[allow(arithmetic_overflow)]
    pub fn get_uint56_pos_big_endian(&self, pos: usize) -> Result<u64, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 6) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            let i3 = self.buffer[position + 3];
            let i4 = self.buffer[position + 4];
            let i5 = self.buffer[position + 5];
            let i6 = self.buffer[position + 6];
            return Result::Ok((i6 as u64 | (i5 as u64) << 8 | (i4 as u64) << 16
                | (i3 as u64) << 24 | (i2 as u64) < 32 | (i1 as u64) << 40 | (i as u64) << 48)
                as u64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_uint56_big_endian(&mut self) -> Result<u64, String> {
        if self.check_ge(6) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            let i3 = self.buffer[self.position];
            self.position += 1;
            let i4 = self.buffer[self.position];
            self.position += 1;
            let i5 = self.buffer[self.position];
            self.position += 1;
            let i6 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i6 as u64 | (i5 as u64) << 8 | (i4 as u64) << 16
                | (i3 as u64) << 24 | (i2 as u64) < 32 | (i1 as u64) << 40 | (i as u64) << 48)
                as u64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }


    #[allow(arithmetic_overflow)]
    pub fn get_int64_pos(&self, pos: usize) -> Result<i64, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 7) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            let i3 = self.buffer[position + 3];
            let i4 = self.buffer[position + 4];
            let i5 = self.buffer[position + 5];
            let i6 = self.buffer[position + 6];
            let i7 = self.buffer[position + 7];
            return Result::Ok((i as i64 | (i1 as i64) << 8 | (i2 as i64) << 16 | (i3 as i64) << 24
                | (i4 as i64) << 32 | (i5 as i64) << 40 | (i6 as i64) << 48 | (i7 as i64) << 56)
                as i64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_int64(&mut self) -> Result<i64, String> {
        if self.check_ge(7) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            let i3 = self.buffer[self.position];
            self.position += 1;
            let i4 = self.buffer[self.position];
            self.position += 1;
            let i5 = self.buffer[self.position];
            self.position += 1;
            let i6 = self.buffer[self.position];
            self.position += 1;
            let i7 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i as i64 | (i1 as i64) << 8 | (i2 as i64) << 16 | (i3 as i64) << 24
                | (i4 as i64) << 32 | (i5 as i64) << 40 | (i6 as i64) << 48 | (i7 as i64) << 56)
                as i64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_uint64_pos(&mut self, pos: usize) -> Result<u64, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 7) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            let i3 = self.buffer[position + 3];
            let i4 = self.buffer[position + 4];
            let i5 = self.buffer[position + 5];
            let i6 = self.buffer[position + 6];
            let i7 = self.buffer[position + 7];
            return Result::Ok((i as u64 | (i1 as u64) << 8 | (i2 as u64) << 16 | (i3 as u64) << 24
                | (i4 as u64) << 32 | (i5 as u64) << 40 | (i6 as u64) << 48 | (i7 as u64) << 56)
                as u64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_uint64(&mut self) -> Result<u64, String> {
        if self.check_ge(7) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            let i3 = self.buffer[self.position];
            self.position += 1;
            let i4 = self.buffer[self.position];
            self.position += 1;
            let i5 = self.buffer[self.position];
            self.position += 1;
            let i6 = self.buffer[self.position];
            self.position += 1;
            let i7 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i as u64 | (i1 as u64) << 8 | (i2 as u64) << 16 | (i3 as u64) << 24 | (i4 as u64) << 32 | (i5 as u64) << 40 | (i6 as u64) << 48 | (i7 as u64) << 56) as u64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_int64_pos_big_endian(&self, pos: usize) -> Result<i64, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 7) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            let i3 = self.buffer[position + 3];
            let i4 = self.buffer[position + 4];
            let i5 = self.buffer[position + 5];
            let i6 = self.buffer[position + 6];
            let i7 = self.buffer[position + 7];
            return Result::Ok((i7 as i64 | (i6 as i64) << 8 | (i5 as i64) << 16 | (i4 as i64) << 24
                | (i3 as i64) << 32 | (i2 as i64) < 40 | (i1 as i64) << 48 | (i as i64) << 56)
                as i64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_int64_big_endian(&mut self) -> Result<i64, String> {
        if self.check_ge(7) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            let i3 = self.buffer[self.position];
            self.position += 1;
            let i4 = self.buffer[self.position];
            self.position += 1;
            let i5 = self.buffer[self.position];
            self.position += 1;
            let i6 = self.buffer[self.position];
            self.position += 1;
            let i7 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i7 as i64 | (i6 as i64) << 8 | (i5 as i64) << 16 | (i4 as i64) << 24
                | (i3 as i64) << 32 | (i2 as i64) < 40 | (i1 as i64) << 48 | (i as i64) << 56)
                as i64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }

    #[allow(arithmetic_overflow)]
    pub fn get_uint64_pos_big_endian(&self, pos: usize) -> Result<u64, String> {
        let position = self.origin + pos;
        if self.check_pos_ge(pos, 7) {
            let i = self.buffer[position];
            let i1 = self.buffer[position + 1];
            let i2 = self.buffer[position + 2];
            let i3 = self.buffer[position + 3];
            let i4 = self.buffer[position + 4];
            let i5 = self.buffer[position + 5];
            let i6 = self.buffer[position + 6];
            let i7 = self.buffer[position + 7];
            return Result::Ok((i7 as u64 | (i6 as u64) << 8 | (i5 as u64) << 16 | (i4 as u64) << 24
                | (i3 as u64) << 32 | (i2 as u64) < 40 | (i1 as u64) << 48 | (i as u64) << 56)
                as u64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", pos)));
    }
    #[allow(arithmetic_overflow)]
    pub fn get_uint64_big_endian(&mut self) -> Result<u64, String> {
        if self.check_ge(7) {
            let i = self.buffer[self.position];
            self.position += 1;
            let i1 = self.buffer[self.position];
            self.position += 1;
            let i2 = self.buffer[self.position];
            self.position += 1;
            let i3 = self.buffer[self.position];
            self.position += 1;
            let i4 = self.buffer[self.position];
            self.position += 1;
            let i5 = self.buffer[self.position];
            self.position += 1;
            let i6 = self.buffer[self.position];
            self.position += 1;
            let i7 = self.buffer[self.position];
            self.position += 1;
            return Result::Ok((i7 as u64 | (i6 as u64) << 8 | (i5 as u64) << 16 | (i4 as u64) << 24
                | (i3 as u64) << 32 | (i2 as u64) < 40 | (i1 as u64) << 48 | (i as u64) << 56)
                as u64);
        }
        return Result::Err(String::from(format!("limit excceed: {}", self.position - self.origin)));
    }


    pub fn get_float32_pos(&mut self, pos: usize) -> f32 {
        unsafe { mem::transmute::<i32, f32>(self.get_int32_pos(pos).unwrap()) }
    }

    pub fn get_float32(&mut self) -> f32 {
        unsafe { mem::transmute::<i32, f32>(self.get_int32().unwrap()) }
    }

    pub fn get_double64_pos(&self, pos: usize) -> f64 {
        unsafe { mem::transmute::<i64, f64>(self.get_int64_pos(pos).unwrap()) }
    }

    pub fn get_double64(&mut self) -> f64 {
        unsafe { mem::transmute::<i64, f64>(self.get_int64().unwrap()) }
    }
    fn check_ge(&self, len: usize) -> bool {
        !(self.position + len >= self.origin + self.limit)
    }
    fn check_gre(&self, len: usize) -> bool {
        !(self.position + len > self.origin + self.limit)
    }
    fn check_pos_ge(&self, pos: usize, len: usize) -> bool {
        !(pos + len >= self.limit)
    }
    fn check_pos_gre(&self, pos: usize, len: usize) -> bool {
        !(pos + len > self.limit)
    }
    pub fn get_packed_i64_pos(&mut self, pos: usize) -> i64 {
        let lead = self.get_uint8_pos(pos).unwrap();
        if lead < 251 {
            return lead as i64;
        }

        return match lead {
            251 =>
                NULL_LENGTH,
            252 =>
                self.get_uint16_pos(pos + 1).unwrap() as i64,
            253 =>
                self.get_uint24_pos(pos + 1).unwrap() as i64,
            _ =>
                self.get_uint32_pos(pos + 1).unwrap() as i64
        };
    }

    pub fn get_packed_i64(&mut self) -> i64 {
        let lead = self.get_uint8().unwrap();
        if lead < 251 {
            return lead as i64;
        }
        return match lead {
            251 =>
                NULL_LENGTH,
            252 =>
                self.get_uint16().unwrap() as i64,
            253 =>
                self.get_uint24().unwrap() as i64,
            _ => {
                let value = self.get_uint32().unwrap() as i64;
                self.position += 4;
                value
            }
        };
    }
    pub fn get_fix_string_pos_len(&mut self, pos: usize, len: usize) -> Option<String> {
        return self.get_fix_string_pos_len_coding(pos, len);
    }

    pub fn get_fix_string_len(&mut self, len: usize) -> Option<String> {
        return self.get_fix_string_len_coding(len);
    }

    pub fn get_fix_string_utf8(&mut self, len: usize) -> Option<String> {
        return self.get_fix_string_len_coding(len);
    }

    pub fn get_fix_string_pos_len_coding(&mut self, pos: usize, len: usize) -> Option<String> {
        if self.check_pos_gre(pos, len) {
            let from = self.origin + pos;
            let end = from + len;
            let mut found = from;
            while (found < end) && self.buffer[found] as char != '\0' {
                found += 1;
            }
            let body = &self.buffer[from..found - from];
            Option::Some(ISO_8859_1.decode(body, DecoderTrap::Strict).unwrap())
        } else {
            Option::None
        }
    }

    pub fn get_fix_string_len_coding(&mut self, len: usize) -> Option<String> {
        if self.check_gre(len) {
            let from = self.position;
            let end = from + len;
            let mut found = from;
            while (found < end) && self.buffer[found] as char != '\0' {
                found += 1;
            }
            let body = &self.buffer[from..found];
            self.position += len;
            Option::Some(ISO_8859_1.decode(body, DecoderTrap::Strict).unwrap())
        } else {
            Option::None
        }
    }

    pub fn get_full_string_pos_len(&self, pos: usize, len: usize) -> Option<String> {
        if self.check_pos_gre(pos, len) {
            let body = &self.buffer[self.origin + pos..self.origin + pos + len];
            Option::Some(ISO_8859_1.decode(body, DecoderTrap::Strict).unwrap())
        } else {
            Option::None
        }
    }

    pub fn get_full_string_len(&mut self, len: usize) -> Option<String> {
        if self.check_gre(len) {
            let body = &self.buffer[self.position..len];
            let value = Option::Some(ISO_8859_1.decode(body, DecoderTrap::Strict).unwrap());
            self.position += len;
            value
        } else {
            Option::None
        }
    }

    pub fn get_string_pos(&self, pos: usize) -> Result<Option<String>, String> {
        if pos >= self.limit {
            return Result::Err(format!("limit excceed: {} ", pos));
        }
        let len = 0xff & self.buffer[self.origin + pos] as usize;
        if pos + len + 1 > self.limit {
            return Result::Err(format!("limit excceed: {}", (pos + len + 1)));
        }
        Result::Ok(Option::Some(ISO_8859_1.decode(&self.buffer[self.origin + pos + 1..len], DecoderTrap::Strict).unwrap()))
    }

    pub fn get_string(&mut self) -> Result<Option<String>, String> {
        if self.position >= self.origin + self.limit {
            return Result::Err(format!("limit excceed: {} ", self.position));
        }
        let len = 0xff & self.buffer[self.position] as usize;
        if self.position + len + 1 >= self.origin + self.limit {
            return Result::Err(format!("limit excceed: {} ", (self.position + len + 1 - self.origin)));
        }
        let start = self.position + 1;
        let string = ISO_8859_1.decode(&self.buffer[start..start + len], DecoderTrap::Strict).unwrap();
        self.position += len + 1;
        Result::Ok(Option::Some(string))
    }


    fn get_int16_be(buffer: &[u8], pos: usize) -> i16 {
        let i = buffer[pos];
        let i1 = buffer[pos + 1];
        return (i as i16) << 8 | i1 as i16;
    }

    pub fn get_int24_be(buffer: &[u8], pos: usize) -> i32 {
        let i = buffer[pos];
        let i1 = buffer[pos + 1];
        let i2 = buffer[pos + 2];
        return ((i2 as i32) << 16 | (i1 as i32) << 8 | i as i32) as i32;
    }

    pub fn get_int32_be(buffer: &[u8], pos: usize) -> i32 {
        let i = buffer[pos];
        let i1 = buffer[pos + 1];
        let i2 = buffer[pos + 2];
        let i3 = buffer[pos + 3];
        return ((i3 as i32) << 24 | (i2 as i32) << 16 | (i1 as i32) << 8 | i as i32) as i32;
    }
    pub fn get_decimal_pos(&mut self, pos: usize, precision: usize, scale: usize) -> Result<BigDecimal, String> {
        let intg = precision - scale;
        let frac = scale;
        let intg0 = intg / DIG_PER_INT32;
        let frac0 = frac / DIG_PER_INT32;
        let intg0x = intg - intg0 * DIG_PER_INT32;
        let frac0x = frac - frac0 * DIG_PER_INT32;

        let bin_size = intg0 * SIZE_OF_INT32 + (DIG2BYTES[intg0x] as usize) + frac0 * SIZE_OF_INT32 + (DIG2BYTES[frac0x] as usize);
        // let limit = if pos < 0 {
        //     pos
        // } else {
        //     pos + bin_size
        // };
        let limit = pos + bin_size;
        if pos + bin_size > self.limit {
            return Result::Err(format!("limit excceed: {}", limit));
        }
        return self.get_decimal0(self.origin + pos, intg, frac, intg0, frac0, intg0x, frac0x);
    }

    pub fn get_decimal(&mut self, precision: usize, scale: usize) -> Result<BigDecimal, String> {
        let intg = precision - scale;
        let frac = scale;
        let intg0 = intg / DIG_PER_INT32;
        let frac0 = frac / DIG_PER_INT32;
        let intg0x = intg - intg0 * DIG_PER_INT32;
        let frac0x = frac - frac0 * DIG_PER_INT32;

        let bin_size = intg0 * SIZE_OF_INT32 + DIG2BYTES[intg0x] + frac0 * SIZE_OF_INT32 + DIG2BYTES[frac0x];

        if self.position + bin_size > self.origin + self.limit {
            return Result::Err(format!("limit excceed: {}", (self.position + bin_size - self.origin)));
        }
        let decimal = self.get_decimal0(self.position, intg, frac, intg0, frac0, intg0x, frac0x);
        self.position += bin_size;
        decimal
    }

    fn get_decimal0(&mut self, begin: usize, intg: usize, frac: usize, intg0: usize, frac0: usize, intg0x: usize, frac0x: usize) -> Result<BigDecimal, String> {
        let mask = if (self.buffer[begin] & 0x80) == 0x80 {
            0
        } else {
            -1
        };

        let mut from = begin;
        // let len = (if mask != 0 { 1 } else { 0 }) + (if intg != 0 { intg } else { 1 }) + (if frac != 0 { 1 } else { 0 });
        let mut buf = vec![];
        if mask != 0 {
            buf.push('-')
        }

        let mut d_copy = self.buffer.clone();
        d_copy.clone()[begin] ^= 0x80;
        let mut mark = buf.len();
        if intg0x != 0 {
            let i = DIG2BYTES[intg0x] as usize;
            let mut x = 0;
            match i {
                1 => x = d_copy[from] as i32,
                2 => x = LogBuffer::get_int16_be(d_copy.as_ref(), from) as i32,
                3 => x = LogBuffer::get_int24_be(d_copy.as_ref(), from) as i32,
                4 => x = LogBuffer::get_int32_be(d_copy.as_ref(), from) as i32,
                _ => {}
            }
            from += i;
            x ^= mask;

            if x < 0 || x >= POWERS10[intg0x + 1] as i32 {
                return Result::Err(format!("bad format, x exceed: {}", POWERS10[intg0x + 1]));
            }

            if x != 0  /* !digit || x != 0 */ {
                let mut j = intg0x;
                while j > 0 {
                    let divisor = POWERS10[j - 1] as i32;
                    let y = x / divisor;
                    if mark < buf.len() || y != 0 {
                        buf.push(('0' as u8 + y as u8) as char)
                    }
                    x -= y * divisor;
                    j -= 1;
                }
            }
        }

        let stop = from + intg0 * SIZE_OF_INT32;
        while from < stop {
            let mut x = LogBuffer::get_int32_be(d_copy.as_ref(), from);
            x ^= mask;
            if x < 0 || x > DIG_MAX {
                return Result::Err(format!("bad format, x exceed: {}", DIG_MAX));
            }

            if x != 0 {
                if mark < buf.len() {
                    let mut i = DIG_PER_DEC1;
                    while i > 0 {
                        let divisor = POWERS10[(i - 1) as usize];
                        let y = (x / divisor as i32) as u8;
                        buf.push(('0' as u8 + y) as char);
                        x -= y as i32 * divisor as i32;
                        i -= 1;
                    }
                } else {
                    let mut i = DIG_PER_DEC1;
                    while i > 0 {
                        let divisor = POWERS10[(i - 1) as usize];
                        let y = x / divisor as i32;
                        if mark < buf.len() || y != 0 {
                            buf.push(('0' as u8 + y as u8) as char)
                        }
                        x -= y * divisor as i32;
                        i -= 1;
                    }
                }
            } else if mark < buf.len() {
                let mut i = DIG_PER_DEC1;

                while i > 0 {
                    buf.push('0');
                    i -= 1;
                }
            }
            from += SIZE_OF_INT32;
        }

        if mark == buf.len() {
            /* fix 0.0 problem, only '.' may cause BigDecimal parsing exception. */
            buf.push('0');
        }

        if frac > 0 {
            buf.push('.');
            mark = buf.len();
            let stop = from + frac0 * SIZE_OF_INT32;
            while from < stop {
                let mut x = LogBuffer::get_int32_be(d_copy.as_ref(), from);
                x ^= mask;

                if x < 0 || x > DIG_MAX {
                    return Result::Err(format!("bad format, x exceed: {}", DIG_MAX));
                }

                if x != 0 {
                    let mut i = DIG_PER_DEC1;
                    while i > 0 {
                        let divisor = POWERS10[(i - 1) as usize];
                        let y = (x / divisor as i32) as u8;
                        buf.push(('0' as u8 + y) as char);
                        x -= y as i32 * divisor as i32;
                        i -= 1;
                    }
                } else {
                    let mut i = DIG_PER_DEC1;
                    while i > 0 {
                        buf.push('0');
                        i -= 1;
                    }
                }
                from += SIZE_OF_INT32;
            }

            if frac0x != 0 {
                let i = DIG2BYTES[frac0x];
                let mut x = 0;
                match i {
                    1 => x = d_copy.clone()[from].clone() as i32,
                    2 => x = LogBuffer::get_int16_be(d_copy.as_ref(), from) as i32,
                    3 => x = LogBuffer::get_int24_be(d_copy.as_ref(), from) as i32,
                    4 => x = LogBuffer::get_int32_be(d_copy.as_ref(), from) as i32,
                    _ => {}
                }
                x ^= mask;
                if x != 0 {
                    let big = DIG_PER_DEC1 - frac0x as i32;
                    x *= POWERS10[big as usize] as i32;
                    if x < 0 || x > DIG_MAX {
                        return Result::Err(format!("bad format, x exceed: {}", DIG_MAX));
                    }

                    let mut j = DIG_PER_DEC1;
                    while j > big {
                        let divisor = POWERS10[(j - 1) as usize];
                        let y = x / divisor as i32;
                        buf.push(('0' as u8 + y as u8) as char);
                        x -= y * divisor as i32;
                        j -= 1;
                    }
                }
            }

            if mark == buf.len() {
                buf.push('0');
            }
        }
        d_copy.clone()[begin] ^= 0x80;
        let decimal = std::str::from_utf8(d_copy.as_ref()).unwrap();
        let decimal = BigDecimal::from_str(decimal).unwrap();
        Result::Ok(decimal)
    }


    pub fn fill_bit_map_pos_map(&self, bit_map: &mut BitSet, pos: usize, len: usize) -> Result<(), String> {
        if pos + ((len + 7) / 8) < self.limit {
            return Result::Err(format!("limit excceed: {}", (pos + (len + 7) / 8)));
        }
        self.fill_bit_map0_pos(bit_map, self.origin + pos, len);
        Result::Ok(())
    }

    pub fn fill_bitmap_map(&mut self, bit_map: &mut BitSet, len: usize) -> Result<(), String> {
        if self.position + ((len + 7) / 8) > self.origin + self.limit {
            return Result::Err(format!("limit excceed: {}", (self.position + (len + 7) / 8 - self.origin)));
        }
        self.position = self.fill_bit_map0_pos(bit_map, self.position, len);
        Result::Ok(())
    }
    fn fill_bit_map0_pos(&self, bit_map: &mut BitSet, mut pos: usize, len: usize) -> usize {
        let buf = self.buffer.clone();
        let mut bit = 0;

        while bit < len {
            let flag = ((buf[pos]) as i32) & 0xff;
            pos += 1;
            if flag == 0 {
                bit += 8;
                continue;
            }
            if flag & 0x01 != 0 { bit_map.insert(bit); }
            if flag & 0x02 != 0 { bit_map.insert(bit + 1); }
            if flag & 0x04 != 0 { bit_map.insert(bit + 2); }
            if flag & 0x08 != 0 { bit_map.insert(bit + 3); }
            if flag & 0x10 != 0 { bit_map.insert(bit + 4); }
            if flag & 0x20 != 0 { bit_map.insert(bit + 5); }
            if flag & 0x40 != 0 { bit_map.insert(bit + 6); }
            if flag & 0x80 != 0 { bit_map.insert(bit + 7); }
            bit += 8;
        }
        return pos;
    }

    pub fn get_bit_map_pos(&self, pos: usize, len: usize) -> BitSet {
        let mut set = BitSet::new();
        self.fill_bit_map_pos_map(&mut set, pos, len).unwrap();
        set
    }

    pub fn get_bit_map(&mut self, len: usize) -> BitSet {
        let mut set = BitSet::new();
        self.fill_bitmap_map(&mut set, len).unwrap();

        set
    }

    pub fn fill_output_pos(&self, pos: usize, len: usize) -> Result<Box<[u8]>, String> {
        if pos + len > self.limit {
            return Result::Err(format!("limit excceed: {}", (pos + len)));
        }
        let x = &self.buffer.clone()[self.origin + pos..len];
        Result::Ok(Box::from(x))
    }
    pub fn fill_output(&self, len: usize) -> Result<Box<[u8]>, String> {
        if self.position + len > self.origin + self.limit {
            return Result::Err(format!("limit excceed: {}", (self.position + len - self.origin)));
        }
        let x = &self.buffer.clone()[self.position..len];
        Result::Ok(Box::from(x))
    }

    pub fn fill_out_bytes_pos(&self, pos: usize, dest: &mut Vec<u8>, dest_pos: usize, len: usize) -> Result<(), String> {
        if pos + len > self.limit {
            return Result::Err(format!("limit excceed: {}", (pos + len)));
        }
        for i in dest_pos..dest_pos + len {
            dest.push(self.buffer[i]);
        }
        Result::Ok(())
    }

    pub fn fill_out_bytes(&mut self, dest: &mut Vec<u8>, dest_pos: usize, len: usize) -> Result<(), String> {
        if self.position + len > self.limit + self.origin {
            return Result::Err(format!("limit excceed: {}", (self.position + len - self.origin)));
        }
        for i in self.position..self.position + len {
            dest.push(self.buffer[i]);
        }
        self.position += len;
        Result::Ok(())
    }

    pub fn get_data_pos_len(&self, pos: usize, len: usize) -> Box<[u8]> {
        let mut data = vec![];
        self.fill_out_bytes_pos(pos, &mut data, 0, len).unwrap();
        Box::from(data)
    }

    pub fn get_data_len(&mut self, len: usize) -> Box<[u8]> {
        let mut data = vec![];
        self.fill_out_bytes(&mut data, 0, len).unwrap();
        Box::from(data)
    }
    pub fn get_data(&mut self) -> Box<[u8]> {
        self.get_data_pos_len(0, self.limit)
    }

    pub fn _hex_dump_pos(&self, pos: usize) -> String {
        if self.limit - pos > 0 {
            let begin = self.origin + pos;
            let end = self.origin + self.limit;
            let buf = self.buffer.clone();
            let mut dump = String::new();
            let i = &buf[begin] >> 4;
            let i1 = &buf[begin] & 0xf;
            dump.push_str(i.to_string().as_str());
            dump.push_str(i1.to_string().as_str());
            let mut i = begin + 1;
            while i < end {
                let j = &buf[begin] >> 4;
                let j1 = &buf[begin] & 0xf;
                dump.push_str(j.to_string().as_str());
                dump.push_str(j1.to_string().as_str());
                i += 1;
            }
            return dump;
        }
        String::new()
    }

    pub fn hex_dump_pos_len(&self, pos: usize, len: usize) -> String {
        if self.limit - pos > 0 {
            let begin = self.origin + pos;
            let end = (self.origin + self.limit).min(begin + len);

            let buf = self.buffer.clone();
            let mut dump = String::new();
            let i = &buf[begin] >> 4;
            let i1 = &buf[begin] & 0xf;
            dump.push_str(i.to_string().as_str());
            dump.push_str(i1.to_string().as_str());
            let mut i = begin + 1;
            while i < end {
                let j = &buf[begin] >> 4;
                let j1 = &buf[begin] & 0xf;
                dump.push_str(j.to_string().as_str());
                dump.push_str(j1.to_string().as_str());
                i += 1;
            }
            return dump;
        }
        String::new()
    }
    pub fn new() -> Self {
        Self { buffer: vec![], origin: 0, limit: 0, position: 0, seminal: 0 }
    }
    pub fn seminal(&self) -> u8 {
        self.seminal
    }
}


pub struct DirectLogFetcher<'a> {
    log_buffer: LogBuffer,
    factor: f32,
    channel: Option<&'a mut Box<dyn TcpSocketChannel>>,
    isem: bool,
}

impl<'a> DirectLogFetcher<'a> {
    const DEFAULT_INITIAL_CAPACITY: usize = 8192;
    const DEFAULT_GROWTH_FACTOR: f32 = 2.0;
    const BIN_LOG_HEADER_SIZE: u32 = 4;
    pub const MASTER_HEARTBEAT_PERIOD_NANOSECOND: u64 = 15000000000;
    pub const MASTER_HEARTBEAT_PERIOD_SECONDS: u32 = 15;
    const READ_TIMEOUT_MILLISECONDS: u32 = (Self::MASTER_HEARTBEAT_PERIOD_SECONDS + 10) * 1000;
    const COM_BINLOG_DUMP: u32 = 18;
    const NET_HEADER_SIZE: usize = 4;
    const SQLSTATE_LENGTH: usize = 5;
    const PACKET_LEN_OFFSET: usize = 0;
    const PACKET_SEQ_OFFSET: usize = 3;
    const MAX_PACKET_LENGTH: usize = 256 * 256 * 256 - 1;

    pub fn new() -> Self {
        Self {
            log_buffer: LogBuffer {
                buffer: vec![],
                origin: 0,
                limit: 0,
                position: 0,
                seminal: 0,
            },
            factor: Self::DEFAULT_GROWTH_FACTOR,
            channel: Option::None,
            isem: false,
        }
    }

    pub fn from_init_capacity(init_capacity: u32) -> DirectLogFetcher<'a> {
        DirectLogFetcher::from_init_factor(init_capacity, Self::DEFAULT_GROWTH_FACTOR)
    }

    pub fn from_init_factor(init_capacity: u32, growth_factor: f32) -> DirectLogFetcher<'a> {
        DirectLogFetcher {
            log_buffer: LogBuffer {
                buffer: vec![],
                origin: 0,
                limit: 0,
                position: 0,
                seminal: 0,
            },
            factor: growth_factor,
            channel: Option::None,
            isem: false,
        }
    }

    pub fn start(&mut self, channel: Option<&'a mut Box<dyn TcpSocketChannel>>) {
        self.channel = channel;
    }

    pub fn fetch(&mut self) -> Result<bool, String> {
        if !self.fetch0(0, Self::NET_HEADER_SIZE) {
            println!("Reached end of input stream while fetching header");
            return Result::Ok(false);
        }

        let mut net_len = self.log_buffer.get_uint24_pos(Self::PACKET_LEN_OFFSET)?;
        let mut net_num = self.log_buffer.get_uint8_pos(Self::PACKET_LEN_OFFSET)?;
        if !self.fetch0(Self::NET_HEADER_SIZE, net_len as usize) {
            println!("{}", format!("Reached end of input stream: packet # {}, len = {}", net_num, net_len));
            return Result::Ok(false);
        }

        let mark = self.log_buffer.get_uint8_pos(Self::NET_HEADER_SIZE)?;
        if mark != 0 {
            if mark == 255 {
                self.log_buffer.position = Self::NET_HEADER_SIZE + 1;
                let error = self.log_buffer.get_int16()?;
                let sql_state = self.log_buffer.forward(1)?.get_fix_string_len(Self::SQLSTATE_LENGTH).unwrap();
                let err_msg = self.log_buffer.get_fix_string_len(self.log_buffer.limit - self.log_buffer.position).unwrap();
                return Result::Err(format!("Received error packet: errno = {}, sqlstate = {} errmsg = {}", error, sql_state, err_msg));
            } else if 254 == mark {
                println!("Received EOF packet from server, apparent master disconnected. It's may be duplicate slaveId , check instance config");
                return Result::Ok(false);
            } else {
                println!("Unexpected response {} while fetching binlog: packet #{}, len: {} ", mark, net_num, net_len)
            }
        }

        if self.isem {
            let semimark = self.log_buffer.get_uint8_pos(Self::NET_HEADER_SIZE + 1)?;
            let semival = self.log_buffer.get_uint8_pos(Self::NET_HEADER_SIZE + 1)?;
            self.log_buffer.seminal = semival;
        }

        while net_len == Self::MAX_PACKET_LENGTH as u32 {
            if !self.fetch0(0, Self::MAX_PACKET_LENGTH) {
                println!("Reached end of input stream while fetching header");
                return Result::Ok(false);
            }
            net_len = self.log_buffer.get_uint24_pos(Self::PACKET_LEN_OFFSET)?;
            net_num = self.log_buffer.get_uint8_pos(Self::PACKET_SEQ_OFFSET)?;

            if !self.fetch0(self.log_buffer.limit, net_len as usize) {
                println!("Reached end of input stream: packet # {}, len: {}", net_num, net_len);
                return Result::Ok(false);
            }
        }
        if self.isem {
            self.log_buffer.origin = Self::NET_HEADER_SIZE + 3;
        } else {
            self.log_buffer.origin = Self::NET_HEADER_SIZE + 1;
        }
        self.log_buffer.position = self.log_buffer.origin;
        self.log_buffer.limit -= self.log_buffer.origin;
        Result::Ok(true)
    }
    fn fetch0(&mut self, off: usize, len: usize) -> bool {
        if self.log_buffer.buffer.len() < off + len {
            for i in 0..len + off - self.log_buffer.buffer.len() {
                self.log_buffer.buffer.push(0)
            }
        }

        let size = self.channel.as_mut().unwrap().read_offset_len(&mut self.log_buffer.buffer, off, len).unwrap();

        if self.log_buffer.limit < off + size {
            self.log_buffer.limit = off + size;
        }
        return true;
    }


    pub fn log_buffer(&mut self) -> &mut LogBuffer {
        &mut self.log_buffer
    }
}


fn copy_of_range(buffer: &Vec<u8>, from: usize, to: usize) -> Vec<u8> {
    let mut bytes = vec![];
    for i in from..to {
        bytes.push(buffer[i])
    }
    bytes
}

struct RowsLogBuffer {
    buffer: LogBuffer,
    column_len: usize,
    json_column_count: i32,
    charset_name: String,
    null_bits: BitSet,
    null_bit_index: usize,
    partial: bool,
    partial_bits: BitSet,
    f_null: bool,
    java_type: i32,
    length: i32,
    value: Serializable,
}


impl RowsLogBuffer {
    const DATETIMEF_INT_OFS: i64 = 0x8000000000;
    const TIMEF_INT_OFS: u64 = 0x800000;
    const TIMEF_OFS: u64 = 0x800000000000;
    const DIGITS: [char; 10] = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'];


    pub fn from(log_buf: LogBuffer, column_len: usize, json_column_count: i32, charset_name: String, partial: bool) -> Self {
        Self {
            buffer: log_buf,
            column_len,
            json_column_count,
            charset_name,
            null_bits: BitSet::default(),
            null_bit_index: 0,
            partial,
            partial_bits: BitSet::default(),
            f_null: false,
            java_type: 0,
            length: 0,
            value: Serializable::Null,
        }
    }

    pub fn next_one_row(&mut self, columns: BitSet) -> bool {
        self.next_one_row_after(columns, false)
    }

    pub fn next_one_row_after(&mut self, columns: BitSet, after: bool) -> bool {
        let has_one_row = self.buffer.has_remaining();

        if has_one_row {
            let mut column = 0;

            for i in 0..self.column_len {
                if columns.contains(i) {
                    column += 1;
                }
            }

            if after && self.partial {
                self.partial_bits.clear();
                let value_options = self.buffer.get_packed_i64();
                let partial_json_updates = 1;
                if (value_options & partial_json_updates) != 0 {
                    self.partial_bits.insert(1);
                    self.buffer.forward(((self.json_column_count + 7) / 8) as usize);
                }
            }

            self.null_bit_index = 0;
            self.null_bits.clear();
            self.buffer.fill_bitmap_map(&mut self.null_bits, column);
        }
        has_one_row
    }

    pub fn next_value(&mut self, colum_name: String, column_index: usize, kind: i32, meta: i32) -> Serializable {
        self.next_value_is_binary(colum_name, column_index, kind, meta, false)
    }

    pub fn next_value_is_binary(&mut self, colum_name: String, column_index: usize, kind: i32, meta: i32, is_binary: bool) -> Serializable {
        self.f_null = self.null_bits.contains(self.null_bit_index);
        self.null_bit_index += 1;

        return if self.f_null {
            self.value = Serializable::Null;
            self.java_type = Self::mysql_to_java_type(kind, meta, is_binary);
            self.length = 0;
            Serializable::Null
        } else {
            self.fetch_value(colum_name, column_index, kind, meta, is_binary).unwrap()
        };
    }

    fn mysql_to_java_type(mut kind: i32, meta: i32, is_binary: bool) -> i32 {
        let java_type;

        if kind == Event::MYSQL_TYPE_STRING {
            if meta >= 256 {
                let byte0 = meta >> 8;
                if byte0 & 0x30 != 0x30 {
                    kind = byte0 | 0x30;
                } else {
                    match byte0 {
                        Event::MYSQL_TYPE_SET |
                        Event::MYSQL_TYPE_ENUM |
                        Event::MYSQL_TYPE_STRING => {
                            kind = byte0;
                        }
                        _ => {}
                    }
                }
            }
        }

        match kind {
            Event::MYSQL_TYPE_LONG => {
                java_type = Types::INTEGER;
            }
            Event::MYSQL_TYPE_TINY => {
                java_type = Types::TINYINT;
            }
            Event::MYSQL_TYPE_SHORT => {
                java_type = Types::SMALLINT;
            }
            Event::MYSQL_TYPE_INT24 => {
                java_type = Types::INTEGER;
            }
            Event::MYSQL_TYPE_LONGLONG => {
                java_type = Types::BIGINT;
            }
            Event::MYSQL_TYPE_DECIMAL => {
                java_type = Types::DECIMAL;
            }
            Event::MYSQL_TYPE_NEWDECIMAL => {
                java_type = Types::DECIMAL;
            }
            Event::MYSQL_TYPE_FLOAT => {
                java_type = Types::REAL;
            }
            Event::MYSQL_TYPE_DOUBLE => {
                java_type = Types::DOUBLE;
            }
            Event::MYSQL_TYPE_BIT => {
                java_type = Types::BIT;
            }
            Event::MYSQL_TYPE_TIMESTAMP |
            Event::MYSQL_TYPE_DATETIME |
            Event::MYSQL_TYPE_TIMESTAMP2 |
            Event::MYSQL_TYPE_DATETIME2 => {
                java_type = Types::TIMESTAMP;
            }
            Event::MYSQL_TYPE_TIME |
            Event::MYSQL_TYPE_TIME2 => {
                java_type = Types::TIME;
            }
            Event::MYSQL_TYPE_NEWDATE |
            Event::MYSQL_TYPE_DATE => {
                java_type = Types::DATE;
            }
            Event::MYSQL_TYPE_YEAR => {
                java_type = Types::VARCHAR;
            }
            Event::MYSQL_TYPE_ENUM => {
                java_type = Types::INTEGER;
            }
            Event::MYSQL_TYPE_SET => {
                java_type = Types::BINARY;
            }
            Event::MYSQL_TYPE_TINY_BLOB |
            Event::MYSQL_TYPE_MEDIUM_BLOB |
            Event::MYSQL_TYPE_LONG_BLOB |
            Event::MYSQL_TYPE_BLOB => {
                if meta == 1 {
                    java_type = Types::VARBINARY;
                } else {
                    java_type = Types::LONGVARBINARY;
                }
            }
            Event::MYSQL_TYPE_VARCHAR |
            Event::MYSQL_TYPE_VAR_STRING => {
                if is_binary {
                    java_type = Types::VARBINARY;
                } else {
                    java_type = Types::VARCHAR;
                }
            }
            Event::MYSQL_TYPE_STRING => {
                if is_binary {
                    java_type = Types::BINARY;
                } else {
                    java_type = Types::CHAR;
                }
            }
            Event::MYSQL_TYPE_GEOMETRY => {
                java_type = Types::BINARY;
            }

            _ => {
                java_type = Types::OTHER;
            }
        }
        java_type
    }

    fn fetch_value(&mut self, colum_name: String, column_index: usize, mut kind: i32, meta: i32, is_binary: bool) -> StringResult<Serializable> {
        let mut len = 0;
        if kind == Event::MYSQL_TYPE_STRING as i32 {
            if meta >= 256 {
                let byte0 = (meta >> 8);
                let byte1 = (meta & 0xff);
                if byte0 & 0x30 != 0x30 {
                    len = byte1 | ((byte0 & 0x30) ^ 0x30 << 4);
                    kind = byte0 | 0x30;
                } else {
                    match byte0 {
                        Event::MYSQL_TYPE_SET |
                        Event::MYSQL_TYPE_ENUM |
                        Event::MYSQL_TYPE_STRING => {
                            kind = byte0;
                            len = byte1;
                        }
                        _ => {
                            return Result::Err(format!("!! Don't know how to handle column type={} meta={} ({})", kind, meta, meta));
                        }
                    }
                }
            } else {
                len = meta;
            }
        }

        match kind {
            Event::MYSQL_TYPE_LONG => {
                self.value = Serializable::I32(self.buffer.get_int32().unwrap());
                self.java_type = Types::INTEGER;
                self.length = 4;
            }
            Event::MYSQL_TYPE_TINY => {
                self.value = Serializable::I8(self.buffer.get_int8().unwrap());
                self.java_type = Types::TINYINT;
                self.length = 1;
            }
            Event::MYSQL_TYPE_SHORT => {
                // XXX: How to check signed / unsigned?
                // self.value = Integer.self.valueOf(unsigned ? buffer.getUint16() :
                // buffer.getInt16());
                self.value = Serializable::I16(self.buffer.get_int16().unwrap());
                self.java_type = Types::SMALLINT; // java.sql.Types.INTEGER;
                self.length = 2;
            }
            Event::MYSQL_TYPE_INT24 => {
                // XXX: How to check signed / unsigned?
                // self.value = Integer.self.valueOf(unsigned ? buffer.getUint24() :
                // buffer.getInt24());
                self.value = Serializable::I32(self.buffer.get_int24().unwrap());
                self.java_type = Types::INTEGER;
                self.length = 3;
            }
            Event::MYSQL_TYPE_LONGLONG => {
                // XXX: How to check signed / unsigned?
                // self.value = unsigned ? buffer.getUlong64()) :
                // Long.self.valueOf(buffer.getLong64());
                self.value = Serializable::I64(self.buffer.get_int64().unwrap());
                self.java_type = Types::BIGINT; // Types.INTEGER;
                self.length = 8;
            }
            Event::MYSQL_TYPE_DECIMAL => {
                println!("MYSQL_TYPE_DECIMAL : This enumeration value is only used internally and cannot exist in a binlog!");
                self.java_type = Types::DECIMAL;
                self.value = Serializable::Null;
                self.length = 0;
            }
            Event::MYSQL_TYPE_NEWDECIMAL => {
                let precision = meta >> 8;
                let decimals = (meta & 0xff) as usize;
                self.buffer.get_decimal(precision as usize, decimals);
                self.java_type = Types::DECIMAL;
                self.length = precision as i32;
            }
            Event::MYSQL_TYPE_FLOAT => {
                self.value = Serializable::I32(self.buffer.get_int32().unwrap());
                self.java_type = Types::REAL;
                self.length = 4;
            }
            Event::MYSQL_TYPE_DOUBLE => {
                self.value = Serializable::F64(self.buffer.get_double64());
                self.java_type = Types::DOUBLE;
                self.length = 8;
            }
            Event::MYSQL_TYPE_BIT => {
                let nbits = ((meta >> 8) * 8) + (meta & 0xff);
                len = (nbits + 7) / 8;
                if nbits > 1 {
                    match len {
                        1 => {
                            self.value = Serializable::U8(self.buffer.get_uint8().unwrap());
                        }
                        2 => {
                            self.value = Serializable::U16(self.buffer.get_uint16().unwrap());
                        }
                        3 => {
                            self.value = Serializable::U32(self.buffer.get_uint24().unwrap());
                        }
                        4 => {
                            self.value = Serializable::U32(self.buffer.get_uint32().unwrap());
                        }
                        5 => {
                            self.value = Serializable::U64(self.buffer.get_uint40().unwrap());
                        }
                        6 => {
                            self.value = Serializable::U64(self.buffer.get_uint48().unwrap());
                        }
                        7 => {
                            self.value = Serializable::U64(self.buffer.get_uint56().unwrap());
                        }
                        8 => {
                            self.value = Serializable::U64(self.buffer.get_uint64().unwrap());
                        }
                        _ => {
                            return Result::Err(format!("!! Unknown Bit len: {}", len));
                        }
                    }
                } else {
                    let bit = self.buffer.get_int8().unwrap();
                    self.value = Serializable::I8(bit);
                }
                self.java_type = Types::BIT;
                self.length = nbits;
            }
            Event::MYSQL_TYPE_TIMESTAMP => {
                // MYSQL DataTypes: TIMESTAMP
                // range is '1970-01-01 00:00:01' UTC to '2038-01-19 03:14:07'
                // UTC
                // A TIMESTAMP cannot represent the value '1970-01-01 00:00:00'
                // because that is equivalent to 0 seconds from the epoch and
                // the value 0 is reserved for representing '0000-00-00
                // 00:00:00', the “zero” TIMESTAMP value.
                let i32 = self.buffer.get_uint32().unwrap();
                if i32 == 0 {
                    self.value = Serializable::String(String::from("0000-00-00 00:00:00"))
                } else {
                    self.value = Serializable::String(timestamp_to_time(i32 as u64));
                }
                self.java_type = Types::TIMESTAMP;
                self.length = 64;
            }
            Event::MYSQL_TYPE_TIMESTAMP2 => {
                let tv_sec = self.buffer.get_uint32_big_endian().unwrap();
                let mut tv_usec = 0;
                match meta {
                    0 => {
                        tv_usec = 0;
                    }
                    1 | 2 => {
                        tv_usec = (self.buffer.get_int8().unwrap() as i32 * 1000);
                    }
                    3 | 4 => {
                        tv_usec = (self.buffer.get_int16_big_endian().unwrap() as i32 * 100);
                    }
                    5 | 6 => {
                        tv_usec = self.buffer.get_int24_big_endian().unwrap();
                    }
                    _ => {
                        tv_usec = 0;
                    }
                }

                let second;
                if tv_sec == 0 {
                    second = Serializable::String(String::from("0000-00-00 00:00:00"));
                } else {
                    second = Serializable::String(timestamp_to_time(tv_usec as u64));
                }

                if meta >= 1 {
                    let mut micro_second = Self::useconds_to_str(tv_usec, meta).unwrap();
                    micro_second = micro_second.substring(0, meta as usize).to_string();
                    self.value = Serializable::String(String::from(format!("{}.{}", second, micro_second)));
                } else {
                    self.value = second;
                }
                self.java_type = Types::TIMESTAMP;
                self.length = 4 + (meta + 1) / 2;
            }
            Event::MYSQL_TYPE_DATETIME => {
                let i64 = self.buffer.get_int64().unwrap();
                if i64 == 0 {
                    self.value = Serializable::String(String::from("0000-00-00 00:00:00"));
                } else {
                    let d = i64 / 1000000;
                    let t = i64 % 1000000;
                    let mut builder = String::new();

                    Self::append_number4(&mut builder, d / 10000);
                    builder.push('-');
                    Self::append_number2(&mut builder, (d % 10000) / 100);
                    builder.push('-');
                    Self::append_number2(&mut builder, d % 100);
                    builder.push(' ');
                    Self::append_number2(&mut builder, t / 10000);
                    builder.push(':');
                    Self::append_number2(&mut builder, (t % 10000) / 100);
                    builder.push(':');
                    Self::append_number2(&mut builder, t % 100);
                    self.value = Serializable::String(builder.to_string());
                }
                self.java_type = Types::TIMESTAMP;
                self.length = 8;
            }
            Event::MYSQL_TYPE_DATETIME2 => {
                let intpart = self.buffer.get_int40_big_endian().unwrap() - Self::DATETIMEF_INT_OFS;
                let mut frac = 0;
                match meta {
                    0 => {
                        frac = 0;
                    }
                    1 | 2 => {
                        frac = self.buffer.get_int8().unwrap() as i32 * 10000;
                    }
                    3 | 4 => {
                        frac = self.buffer.get_int16_big_endian().unwrap() as i32 * 100;
                    }
                    5 | 6 => {
                        frac = self.buffer.get_int24_big_endian().unwrap();
                    }
                    _ => frac = 0
                }

                let second;
                if intpart == 0 {
                    second = String::from("0000-00-00 00:00:00");
                } else {
                    let ymd = intpart >> 17;
                    let ym = ymd >> 5;
                    let hms = intpart % (1 << 17);
                    let mut builder = String::new();

                    Self::append_number4(&mut builder, ym / 13);
                    builder.push('-');
                    Self::append_number2(&mut builder, ym % 13);
                    builder.push('-');
                    Self::append_number2(&mut builder, (ymd % (1 << 5)));
                    builder.push(' ');
                    Self::append_number2(&mut builder, (hms >> 12));
                    builder.push(':');
                    Self::append_number2(&mut builder, ((hms >> 6) % (1 << 6)));
                    builder.push(':');
                    Self::append_number2(&mut builder, (hms % (1 << 6)));
                    second = builder.to_string();
                }

                if meta >= 1 {
                    let mut micro_second = Self::useconds_to_str(frac, meta).unwrap();
                    micro_second = micro_second.substring(0, meta as usize).to_string();
                    self.value = Serializable::String(format!("{}.{}", second, micro_second));
                } else {
                    self.value = Serializable::String(String::from(second));
                }

                self.java_type = Types::TIMESTAMP;
                self.length = 5 + (meta + 1) / 2;
            }
            Event::MYSQL_TYPE_TIME => {
                let i32 = self.buffer.get_int24().unwrap();
                let u32 = if i32 < 0 { -i32 } else { i32 };

                if i32 == 0 {
                    self.value = Serializable::String(String::from("00:00:00"));
                } else {
                    let mut builder = String::new();

                    if i32 < 0 {
                        builder.push('-');
                    }

                    let d = u32 / 10000;
                    if d > 100 {
                        builder.push_str(d.to_string().as_str());
                    } else {
                        Self::append_number2(&mut builder, d as i64);
                    }
                    builder.push(':');
                    Self::append_number2(&mut builder, (u32 as i64 % 10000) / 100);
                    builder.push(':');
                    Self::append_number2(&mut builder, u32 as i64 % 100);
                    self.value = Serializable::String(builder.to_string());
                }
                self.java_type = Types::TIME;
                self.length = 3;
            }
            Event::MYSQL_TYPE_TIME2 => {
                let mut intpart = 0;
                let mut frac = 0;
                let mut ltime = 0;
                match meta {
                    0 => {
                        intpart = self.buffer.get_uint24_big_endian().unwrap() as u64 - Self::TIMEF_INT_OFS;
                        ltime = intpart << 24;
                    }
                    1 | 2 => {
                        intpart = self.buffer.get_uint24_big_endian().unwrap() as u64 - Self::TIMEF_INT_OFS;
                        frac = self.buffer.get_uint8().unwrap() as u64;

                        if intpart < 0 && frac > 0 {
                            /*
                             * Negative values are stored with reverse
                             * fractional part order, for binary sort
                             * compatibility. Disk value intpart frac Time value
                             * Memory value 800000.00 0 0 00:00:00.00
                             * 0000000000.000000 7FFFFF.FF -1 255 -00:00:00.01
                             * FFFFFFFFFF.FFD8F0 7FFFFF.9D -1 99 -00:00:00.99
                             * FFFFFFFFFF.F0E4D0 7FFFFF.00 -1 0 -00:00:01.00
                             * FFFFFFFFFF.000000 7FFFFE.FF -1 255 -00:00:01.01
                             * FFFFFFFFFE.FFD8F0 7FFFFE.F6 -2 246 -00:00:01.10
                             * FFFFFFFFFE.FE7960 Formula to convert fractional
                             * part from disk format (now stored in "frac"
                             * variable) to absolute value: "0x100 - frac". To
                             * reconstruct in-memory value, we shift to the next
                             * integer value and then substruct fractional part.
                             */

                            intpart += 1;
                            frac -= 0x100;
                        }
                        frac = frac * 10000;
                        ltime = intpart << 24;
                    }
                    3 | 4 => {
                        intpart = self.buffer.get_uint24_big_endian().unwrap() as u64 - Self::TIMEF_INT_OFS;
                        frac = self.buffer.get_uint16_big_endian().unwrap() as u64;
                        ltime = intpart << 24;
                    }
                    5 | 6 => {
                        intpart = self.buffer.get_uint48_big_endian().unwrap() - Self::TIMEF_OFS;
                        ltime = intpart;
                        frac = (intpart % (1 << 24));
                    }
                    _ => {
                        intpart = self.buffer.get_uint24_big_endian().unwrap() as u64 - Self::TIMEF_INT_OFS;
                        ltime = intpart << 24;
                    }
                }
                let second;
                if intpart == 0 {
                    second = if frac < 0 { String::from("-00:00:00") } else { String::from("00:00:00") }
                } else {
                    let ultime = ltime;
                    intpart = ultime >> 24;

                    let mut builder = String::new();
                    if ltime < 0 {
                        builder.push('-');
                    }

                    let d = ((intpart >> 12) % (1 << 10));
                    if d >= 100 {
                        builder.push_str(d.to_string().as_str());
                    } else {
                        Self::append_number2(&mut builder, d as i64);
                    }
                    builder.push(':');
                    Self::append_number2(&mut builder, ((intpart as i64 >> 6) % (1 << 6)));
                    builder.push(':');
                    Self::append_number2(&mut builder, (intpart as i64 % (1 << 6)));
                    second = builder;
                }

                if meta > 1 {
                    // let frac = if frac < 0 { -frac } else { frac };
                    let mut micro_second = Self::useconds_to_str(frac as i32, meta).unwrap();
                    micro_second = micro_second.substring(0, meta as usize).to_string();
                    self.value = Serializable::String(String::from(format!("{}.{}", second, micro_second)));
                } else {
                    self.value = Serializable::String(second);
                }

                self.java_type = Types::TIME;
                self.length = 3 + (meta + 1) / 2;
            }
            Event::MYSQL_TYPE_NEWDATE => {
                println!("MYSQL_TYPE_NEWDATE : This enumeration value is only used internally and cannot exist in a binlog!");
                self.java_type = Types::DATE;
                self.value = Serializable::Null;
                self.length = 0;
            }
            Event::MYSQL_TYPE_DATE => {
                let i32 = self.buffer.get_uint24().unwrap() as i64;
                if i32 == 0 {
                    self.value = Serializable::String(String::from("0000-00-00"));
                } else {
                    let mut builder = String::new();
                    Self::append_number4(&mut builder, i32 / (16 * 32));
                    builder.push('-');
                    Self::append_number2(&mut builder, i32 / 32 % 16);
                    builder.push('-');
                    Self::append_number2(&mut builder, i32 % 32);
                    self.value = Serializable::String(builder.to_string());
                }
                self.java_type = Types::DATE;
                self.length = 3;
            }
            Event::MYSQL_TYPE_YEAR => {
                let i32 = self.buffer.get_uint8().unwrap() as i32;
                if i32 == 0 {
                    self.value = Serializable::String(String::from("0000"));
                } else {
                    self.value = Serializable::String((i32 + 1900).to_string());
                }
                self.java_type = Types::VARCHAR;
                self.length = 1;
            }
            Event::MYSQL_TYPE_ENUM => {
                let int32;
                match len {
                    1 => {
                        int32 = self.buffer.get_uint8().unwrap() as u16;
                    }
                    2 => {
                        int32 = self.buffer.get_uint16().unwrap();
                    }
                    _ => {
                        int32 = 0;
                        println!("!! Unknown ENUM pack len: {}", len)
                    }
                }
                self.value = Serializable::String(int32.to_string());
                self.java_type = Types::INTEGER;
                self.length = len;
            }
            Event::MYSQL_TYPE_SET => {
                let nbits = (meta & 0xFF) * 8;
                len = (nbits + 7) / 8;
                if nbits > 1 {
                    match len {
                        1 => {
                            self.value = Serializable::U8(self.buffer.get_uint8().unwrap());
                        }
                        2 => {
                            self.value = Serializable::U16(self.buffer.get_uint16().unwrap());
                        }
                        3 => {
                            self.value = Serializable::U32(self.buffer.get_uint24().unwrap());
                        }
                        4 => {
                            self.value = Serializable::U32(self.buffer.get_uint32().unwrap());
                        }
                        5 => {
                            self.value = Serializable::U64(self.buffer.get_uint40().unwrap());
                        }
                        6 => {
                            self.value = Serializable::U64(self.buffer.get_uint48().unwrap());
                        }
                        7 => {
                            self.value = Serializable::U64(self.buffer.get_uint56().unwrap());
                        }
                        8 => {
                            self.value = Serializable::U64(self.buffer.get_uint64().unwrap());
                        }
                        _ => {
                            println!("!! Unknown Set len: {}", len);
                        }
                    }
                } else {
                    let bit = self.buffer.get_int8().unwrap();
                    self.value = Serializable::I8(bit);
                }
                self.java_type = Types::BIT;
                self.length = len;
            }
            Event::MYSQL_TYPE_TINY_BLOB => {
                println!("MYSQL_TYPE_TINY_BLOB : This enumeration value is only used internally and cannot exist in a binlog!")
            }
            Event::MYSQL_TYPE_MEDIUM_BLOB => {
                println!("MYSQL_TYPE_MEDIUM_BLOB : This enumeration value is only used internally and cannot exist in a binlog!")
            }
            Event::MYSQL_TYPE_LONG_BLOB => {
                println!("MYSQL_TYPE_LONG_BLOB : This enumeration value is only used internally and cannot exist in a binlog!")
            }
            Event::MYSQL_TYPE_BLOB => {
                match meta {
                    1 => {
                        let len8 = self.buffer.get_uint8().unwrap() as usize;
                        let mut binary = vec![];
                        self.buffer.fill_out_bytes(&mut binary, 0, len8);
                        self.value = Serializable::BYTES(binary);
                        self.java_type = Types::VARBINARY;
                        self.length = len8 as i32;
                    }
                    2 => {
                        let len16 = self.buffer.get_uint16().unwrap() as usize;
                        let mut binary = vec![];
                        self.buffer.fill_out_bytes(&mut binary, 0, len16);
                        self.value = Serializable::BYTES(binary);
                        self.java_type = Types::VARBINARY;
                        self.length = len16 as i32;
                    }
                    3 => {
                        let len24 = self.buffer.get_uint16().unwrap() as usize;
                        let mut binary = vec![];
                        self.buffer.fill_out_bytes(&mut binary, 0, len24);
                        self.value = Serializable::BYTES(binary);
                        self.java_type = Types::VARBINARY;
                        self.length = len24 as i32;
                    }
                    4 => {
                        let len32 = self.buffer.get_uint16().unwrap() as usize;
                        let mut binary = vec![];
                        self.buffer.fill_out_bytes(&mut binary, 0, len32);
                        self.value = Serializable::BYTES(binary);
                        self.java_type = Types::VARBINARY;
                        self.length = len32 as i32;
                    }
                    _ => {
                        println!("!! Unknown BLOB packlen: {}", meta);
                    }
                }
            }
            Event::MYSQL_TYPE_VARCHAR |
            Event::MYSQL_TYPE_VAR_STRING => {
                len = meta;
                if len < 256 {
                    len = self.buffer.get_uint8().unwrap() as i32;
                } else {
                    len = self.buffer.get_uint16().unwrap() as i32;
                }

                if is_binary {
                    let mut binary = vec![];
                    self.buffer.fill_out_bytes(&mut binary, 0, len as usize);
                    self.java_type = Types::VARBINARY;
                    self.value = Serializable::BYTES(binary);
                } else {
                    self.value = Serializable::String(self.buffer.get_full_string_len(len as usize).unwrap());
                    self.java_type = Types::VARCHAR;
                }
                self.length = len;
            }
            Event::MYSQL_TYPE_STRING => {
                if len < 256 {
                    len = self.buffer.get_uint8().unwrap() as i32;
                } else {
                    len = self.buffer.get_uint16().unwrap() as i32;
                }

                if is_binary {
                    let mut binary = vec![];
                    self.buffer.fill_out_bytes(&mut binary, 0, len as usize);
                    self.java_type = Types::BINARY;
                    self.value = Serializable::BYTES(binary);
                } else {
                    self.value = Serializable::String(self.buffer.get_full_string_len(len as usize).unwrap());
                    self.java_type = Types::CHAR;
                }
                self.length = len;
            }
            Event::MYSQL_TYPE_JSON => {
                match meta {
                    1 => {
                        len = self.buffer.get_uint8().unwrap() as i32;
                    }
                    2 => {
                        len = self.buffer.get_uint16().unwrap() as i32;
                    }
                    3 => {
                        len = self.buffer.get_uint24().unwrap() as i32;
                    }
                    4 => {
                        len = self.buffer.get_uint32().unwrap() as i32;
                    }
                    _ => {
                        println!("!! Unknown JSON packlen: {}", meta)
                    }
                }
                if self.partial_bits.contains(1) {
                    let position = self.buffer.position();
                    // TODO
                    todo!()
                }
            }
            Event::MYSQL_TYPE_GEOMETRY => {
                match meta {
                    1 => {
                        len = self.buffer.get_uint8().unwrap() as i32;
                    }
                    2 => {
                        len = self.buffer.get_int16().unwrap() as i32;
                    }
                    3 => {
                        len = self.buffer.get_uint24().unwrap() as i32;
                    }
                    4 => {
                        len = self.buffer.get_uint32().unwrap() as i32;
                    }
                    _ => {
                        return Result::Err(String::from(format!("!! Unknown MYSQL_TYPE_GEOMETRY packlen: {}", meta)));
                    }
                }
                let mut binary = vec![];
                self.buffer.fill_out_bytes(&mut binary, 0, len as usize);
                self.java_type = Types::BINARY;
                self.value = Serializable::BYTES(binary);
                self.length = len;
            }
            Event::MYSQL_TYPE_BOOL |
            Event::MYSQL_TYPE_INVALID |
            _ => {
                String::from(format!("!! Don't know how to handle column type: {} meta: {} ({})", kind, meta, meta));
                self.java_type = Types::OTHER;
                self.value = Serializable::Null;
                self.length = 0;

            }
        }
        Result::Ok(self.value.clone())
    }

    fn append_number4(builder: &mut String, d: i64) {
        if d >= 1000 {
            builder.push(Self::DIGITS[d as usize / 1000]);
            builder.push(Self::DIGITS[(d as usize / 1000) % 10]);
            builder.push(Self::DIGITS[(d as usize / 10) % 10]);
            builder.push(Self::DIGITS[d as usize % 10]);
        } else {
            builder.push('0');
            Self::append_number3(builder, d);
        }
    }

    fn append_number3(builder: &mut String, d: i64) {
        if d >= 100 {
            builder.push(Self::DIGITS[d as usize / 100]);
            builder.push(Self::DIGITS[(d as usize / 10) % 10]);
            builder.push(Self::DIGITS[d as usize % 10]);
        } else {
            builder.push('0');
            Self::append_number2(builder, d);
        }
    }

    fn append_number2(builder: &mut String, d: i64) {
        if d >= 10 {
            builder.push(Self::DIGITS[(d as usize / 10) % 10]);
            builder.push(Self::DIGITS[d as usize % 10]);
        } else {
            builder.push('0');
            builder.push(Self::DIGITS[d as usize])
        }
    }
    fn useconds_to_str(frac: i32, meta: i32) -> StringResult<String> {
        let mut sec = frac.to_string();
        if meta > 6 {
            return Result::Err(format!("unknow useconds meta : {}", meta));
        }

        if sec.len() < 6 {
            let mut result = String::new();
            let mut len = 6 - sec.len();
            while len > 0 {
                result.push('0');
                len -= 1;
            }
            result.push_str(sec.as_str());
            sec = result.to_string();
        }

        Result::Ok(sec.substring(0, meta as usize).to_string())
    }
}


