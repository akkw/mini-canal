use std::fmt::{Error, format};
use std::mem;
use std::str::FromStr;
use bigdecimal::BigDecimal;
use bit_set::BitSet;
use encoding::{DecoderTrap, Encoding};
use encoding::all::ISO_8859_1;
use encoding::DecoderTrap::Strict;

const NULL_LENGTH: i64 = -1;
const DIG_PER_DEC1: i32 = 9;
const DIG_BASE: i32 = 1000000000;
const DIG_MAX: i32 = DIG_BASE - 1;
const dig2bytes: [usize; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
const powers10: [usize; 10] = [1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000];
const DIG_PER_INT32: usize = 9;
const SIZE_OF_INT32: usize = 4;

pub struct LogBuffer {
    buffer: Box<[u8]>,
    origin: usize,
    limit: usize,
    position: usize,
    seminal: usize,
}

impl LogBuffer {
    pub fn from(buffer: Box<[u8]>, origin: usize, limit: usize) -> Result<LogBuffer, String> {
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
            buffer: copy_of_range(self.buffer.clone(), off, len),
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
            buffer: copy_of_range(self.buffer.clone(), self.position, end),
            origin: 0,
            limit: len,
            position: 0,
            seminal: 0,
        })
    }

    pub fn duplicate(&self) -> LogBuffer {
        LogBuffer {
            buffer: copy_of_range(self.buffer.clone(), self.origin, self.origin + self.limit),
            origin: 0,
            limit: self.limit,
            position: 0,
            seminal: 0,
        }
    }

    pub fn position(self) -> usize {
        self.position - self.origin
    }

    pub fn up_position(&mut self, new_position: usize) -> Result<bool, String> {
        if new_position > self.limit {
            return Result::Err(String::from(format!("limit excceed: {}", new_position)));
        }
        self.position = self.origin + new_position;
        Result::Ok(true)
    }

    pub fn forward(&mut self, len: usize) -> Result<bool, String> {
        if self.position + len > self.origin + self.limit {
            let position = self.position;
            let origin = self.origin;
            return Result::Err(String::from(format!("limit excceed: {}", position + len - origin)));
        }

        self.position += len;
        Result::Ok(true)
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
            return return Result::Ok((i as i64 | (i1 as i64) << 8 | (i2 as i64) << 16 | (i3 as i64) << 24
                | (i4 as i64) << 32 | (i5 as i64) << 40 | (i6 as i64) << 48) as i64);;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
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
            return return Result::Ok((i as i64 | (i1 as i64) << 8 | (i2 as i64) << 16 | (i3 as i64) << 24
                | (i4 as i64) << 32 | (i5 as i64) << 40 | (i6 as i64) << 48) as i64);;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
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
            return return Result::Ok((i6 as i64 | (i5 as i64) << 8 | (i4 as i64) << 16
                | (i3 as i64) << 24 | (i2 as i64) < 32 | (i1 as i64) << 40 | (i as i64) << 48)
                as i64);;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
            ;
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
            let body = &self.buffer[from..found - from];
            Option::Some(ISO_8859_1.decode(body, DecoderTrap::Strict).unwrap())
        } else {
            Option::None
        }
    }

    pub fn get_full_string_pos_len(&self, pos: usize, len: usize) -> Option<String> {
        if self.check_pos_gre(pos, len) {
            let body = &self.buffer[self.origin + pos..len];
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
        if pos >= self.limit || pos < 0 {
            return Result::Err(format!("limit excceed: {} ", pos));
        }
        let len = 0xff & self.buffer[self.origin + pos] as usize;
        if pos + len + 1 > self.limit {
            return Result::Err(format!("limit excceed: {}", (pos + len + 1)));
        }
        Result::Ok(Option::Some(ISO_8859_1.decode(&self.buffer[self.origin + pos + 1..len], DecoderTrap::Strict).unwrap()))
    }

    pub fn get_string(&self) -> Result<Option<String>, String> {
        if self.position >= self.origin + self.limit {
            return Result::Err(format!("limit excceed: {} ", self.position));
        }
        let len = 0xff & self.buffer[self.position] as usize;
        if self.position + len + 1 >= self.origin + self.limit {
            return Result::Err(format!("limit excceed: {} ", (self.position + len + 1 - self.origin)));
        }
        Result::Ok(Option::Some(ISO_8859_1.decode(&self.buffer[self.position + 1..len], DecoderTrap::Strict).unwrap()))
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

        let binSize = intg0 * SIZE_OF_INT32 + (dig2bytes[intg0x] as usize) + frac0 * SIZE_OF_INT32 + (dig2bytes[frac0x] as usize);
        let limit = if pos < 0 {
            pos
        } else {
            pos + binSize
        };
        if pos + binSize > self.limit {
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

        let binSize = intg0 * SIZE_OF_INT32 + dig2bytes[intg0x] + frac0 * SIZE_OF_INT32 + dig2bytes[frac0x];

        if self.position + binSize > self.origin + self.limit {
            return Result::Err(format!("limit excceed: {}", (self.position + binSize - self.origin)));
        }
        let decimal = self.get_decimal0(self.position, intg, frac, intg0, frac0, intg0x, frac0x);
        self.position += binSize;
        decimal
    }

    fn get_decimal0(&mut self, begin: usize, intg: usize, frac: usize, intg0: usize, frac0: usize, intg0x: usize, frac0x: usize) -> Result<BigDecimal, String> {
        let mask = if (self.buffer[begin] & 0x80) == 0x80 {
            0
        } else {
            -1
        };

        let mut from = begin;
        let len = (if mask != 0 { 1 } else { 0 }) + (if intg != 0 { intg } else { 1 }) + (if frac != 0 { 1 } else { 0 });
        let mut buf = vec![];
        if mask != 0 {
            buf.push('-')
        }

        let mut d_copy = self.buffer.clone();
        d_copy.clone()[begin] ^= 0x80;
        let mut mark = buf.len();
        if intg0x != 0 {
            let i = dig2bytes[intg0x] as usize;
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

            if x < 0 || x >= powers10[intg0x + 1] as i32 {
                return Result::Err(format!("bad format, x exceed: {}", powers10[intg0x + 1]));
            }

            if x != 0  /* !digit || x != 0 */ {
                let mut j = intg0x;
                while j > 0 {
                    let divisor = powers10[j - 1] as i32;
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
                        let divisor = powers10[(i - 1) as usize];
                        let y = (x / divisor as i32) as u8;
                        buf.push(('0' as u8 + y) as char);
                        x -= y as i32 * divisor as i32;
                        i -= 1;
                    }
                } else {
                    let mut i = DIG_PER_DEC1;
                    while i > 0 {
                        let divisor = powers10[(i - 1) as usize];
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
                        let divisor = powers10[(i - 1) as usize];
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
                let i = dig2bytes[frac0x];
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
                    x *= powers10[big as usize] as i32;
                    if x < 0 || x > DIG_MAX {
                        return Result::Err(format!("bad format, x exceed: {}", DIG_MAX));
                    }

                    let mut j = DIG_PER_DEC1;
                    while j > big {
                        let divisor = powers10[(j - 1) as usize];
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


    pub fn fill_bit_map_pos_map(&self, bit_map: &mut bit_set::BitSet, pos: usize, len: usize) -> Result<(), String> {
        if pos + ((len + 7) / 8) < self.limit {
            return Result::Err(format!("limit excceed: {}", (pos + (len + 7) / 8)));
        }
        self.fill_bit_map0_pos(bit_map, self.origin + pos, len);
        Result::Ok(())
    }

    pub fn fill_bitmap_map(&mut self, bit_map: &mut bit_set::BitSet, len: usize) -> Result<(), String> {
        if self.position + ((len + 7) / 8) < self.origin + self.limit {
            return Result::Err(format!("limit excceed: {}", (self.position + (len + 7) / 8 - self.origin)));
        }
        self.position = self.fill_bit_map0_pos(bit_map, self.position, len);
        Result::Ok(())
    }
    fn fill_bit_map0_pos(&self, bit_map: &mut bit_set::BitSet, pos: usize, len: usize) -> usize {
        let buf = self.buffer.clone();
        let mut bit = 0;

        while bit < len {
            let flag = ((buf[pos]) as i32) & 0xff;
            if flag == 0 {
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
        return bit_map.len();
    }

    pub fn get_bit_map_pos(&self, pos: usize, len: usize) -> BitSet {
        let mut set = BitSet::new();
        self.fill_bit_map_pos_map(&mut set, pos, len);
        set
    }

    pub fn get_bit_map(&mut self, pos: usize, len: usize) -> BitSet {
        let mut set = BitSet::new();
        self.fill_bitmap_map(&mut set, len);
        set
    }

    pub fn fillOutputPos(&self, pos: usize, len: usize) -> Result<Box<[u8]>, String> {
        if pos + len > self.limit {
            return Result::Err(format!("limit excceed: {}", (pos + len)));
        }
        let x = &self.buffer.clone()[self.origin + pos..len];
        Result::Ok(Box::from(x))
    }
    pub fn fillOutput(&self, len: usize) -> Result<Box<[u8]>, String> {
        if self.position + len > self.origin + self.limit {
            return Result::Err(format!("limit excceed: {}", (self.position + len - self.origin)));
        }
        let x = &self.buffer.clone()[self.position..len];
        Result::Ok(Box::from(x))
    }

    pub fn fillOutBytesPos(&self, pos: usize, dest: &mut Vec<u8>, dest_pos: usize, len: usize) -> Result<(), String> {
        if pos + len > self.limit {
            return Result::Err(format!("limit excceed: {}", (pos + len)));
        }
        for i in dest_pos..dest_pos + len {
            dest.push(self.buffer[i]);
        }
        Result::Ok(())
    }

    pub fn fillOutBytes(&mut self, dest: &mut Vec<u8>, dest_pos: usize, len: usize) -> Result<(), String> {
        if self.position + len > self.limit + self.origin {
            return Result::Err(format!("limit excceed: {}", (self.position + len - self.origin)));
        }
        for i in self.position..dest_pos + len {
            dest.push(self.buffer[i]);
        }
        self.position += len;
        Result::Ok(())
    }

    pub fn get_data_pos_len(&self, pos: usize, len: usize) -> Box<[u8]> {
        let mut data = vec![];
        self.fillOutBytesPos(pos, &mut data, 0, len).unwrap();
        Box::from(data)
    }

    pub fn get_data_len(&mut self, len: usize) -> Box<[u8]> {
        let mut data = vec![];
        self.fillOutBytes(&mut data, 0, len).unwrap();
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
            let i = (&buf[begin] >> 4);
            let i1 = (&buf[begin] & 0xf);
            dump.push_str((i.to_string().as_str()));
            dump.push_str((i1.to_string().as_str()));
            let mut i = begin + 1;
            while i < end {
                let j = (&buf[begin] >> 4);
                let j1 = (&buf[begin] & 0xf);
                dump.push_str((j.to_string().as_str()));
                dump.push_str((j1.to_string().as_str()));
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
            let i = (&buf[begin] >> 4);
            let i1 = (&buf[begin] & 0xf);
            dump.push_str((i.to_string().as_str()));
            dump.push_str((i1.to_string().as_str()));
            let mut i = begin + 1;
            while i < end {
                let j = (&buf[begin] >> 4);
                let j1 = (&buf[begin] & 0xf);
                dump.push_str((j.to_string().as_str()));
                dump.push_str((j1.to_string().as_str()));
                i += 1;
            }
            return dump;
        }
        String::new()
    }
}


fn copy_of_range(buffer: Box<[u8]>, from: usize, to: usize) -> Box<[u8]> {
    let mut bytes = vec![];
    for i in from..to {
        bytes.push(buffer[i])
    }
    Box::from(bytes)
}