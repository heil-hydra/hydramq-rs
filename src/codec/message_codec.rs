use message::message::Message;
use message::message::Key;
use message::message::Value;
use message::message::Map;
use message::message::List;
use message::message::Timestamp;
use bytes::{BigEndian, BufMut, BytesMut, Buf};
use std::str;
use std::cell::Cell;
use codec::util;
use uuid::Uuid;
use std::io::Cursor;
use chrono::prelude::*;

pub fn calculate_message_size(message: &Message) -> i32 {
    let calculator = SizeCalculator;
    let mut size = 0;
    calculator.visit_message(message, &mut size);
    size
}

pub fn calculate_key_size(key: &Key) -> i32 {
    let calculator = SizeCalculator;
    let mut size = 0;
    calculator.visit_key(key, &mut size);
    size
}

pub fn calculate_value_size(value: &Value) -> i32 {
    let calculator = SizeCalculator;
    let mut size = 0;
    calculator.visit_value(value, &mut size);
    size
}

pub fn encode_message(message: &Message) -> BytesMut {
    let size = calculate_message_size(message);
    let mut buffer = BytesMut::with_capacity(size as usize);
    let encoder = Encoder;
    encoder.visit_message(message, &mut buffer);
    buffer
}

pub trait MessageVisitor<'a> {
    type Output;

    fn visit_message(&self, value: &'a Message, buffer: &'a mut Self::Output);

    fn visit_map(&self, value: &'a Map, buffer: &'a mut Self::Output);

    fn visit_list(&self, list: &'a List, buffer: &'a mut Self::Output);

    fn visit_key(&self, value: &'a Key, buffer: &'a mut Self::Output);

    fn visit_value(&self, value: &'a Value, buffer: &'a mut Self::Output);

    fn visit_bytes(&self, value: &'a [u8], buffer: &'a mut Self::Output);

    fn visit_i32(&self, value: i32, buffer: &'a mut Self::Output);

    fn visit_i64(&self, value: i64, buffer: &'a mut Self::Output);

    fn visit_f32(&self, value: f32, buffer: &'a mut Self::Output);

    fn visit_f64(&self, value: f64, buffer: &'a mut Self::Output);

    fn visit_bool(&self, value: bool, buffer: &'a mut Self::Output);

    fn visit_str(&self, value: &'a str, buffer: &'a mut Self::Output);

    fn visit_uuid(&self, value: Uuid, buffer: &'a mut Self::Output);

    fn visit_timestamp(&self, value: Timestamp, buffer: &'a mut Self::Output);

    fn visit_null(&self, buffer: &'a mut Self::Output);
}

pub struct SizeCalculator;

impl<'a> MessageVisitor<'a> for SizeCalculator {
    type Output = i32;

    fn visit_message(&self, message: &'a Message, buffer: &'a mut Self::Output) {
        // flags
        *buffer += 4;

        if let Some(_) = message.timestamp() {
            *buffer += 12;
        }

        if let Some(_) = message.expiration() {
            *buffer += 12;
        }

        if let Some(_) = message.correlation_id() {
            *buffer += 16;
        }

        if message.headers().len() > 0 {
            self.visit_map(&message.headers(), buffer);
        }

        if let Some(body) = message.body() {
            self.visit_value(body, buffer);
        }
    }

    fn visit_map(&self, value: &'a Map, buffer: &'a mut Self::Output) {
        *buffer += 4;
        for (key, value) in value.iter() {
            self.visit_key(key, buffer);
            self.visit_value(value, buffer);
        }
    }

    fn visit_list(&self, list: &'a List, buffer: &'a mut Self::Output) {
        *buffer += 4;
        for value in list.iter() {
            self.visit_value(value, buffer);
        }
    }

    fn visit_key(&self, key: &'a Key, buffer: &'a mut Self::Output) {
        *buffer += 1;
        match key {
            Key::Str(ref key) => self.visit_str(key, buffer),
            Key::I32(key) => self.visit_i32(*key, buffer),
        }
    }

    fn visit_value(&self, value: &'a Value, buffer: &'a mut Self::Output) {
        *buffer += 1;
        match value {
            Value::Null => (),
            Value::Str(ref value) => self.visit_str(value, buffer),
            Value::I32(value) => self.visit_i32(*value, buffer),
            Value::I64(value) => self.visit_i64(*value, buffer),
            Value::F32(value) => self.visit_f32(*value, buffer),
            Value::F64(value) => self.visit_f64(*value, buffer),
            Value::Bool(value) => self.visit_bool(*value, buffer),
            Value::Bytes(ref value) => self.visit_bytes(value, buffer),
            Value::Map(ref value) => self.visit_map(value, buffer),
            Value::List(ref value) => self.visit_list(value, buffer),
            Value::Uuid(value) => self.visit_uuid(*value, buffer),
            Value::Timestamp(value) => self.visit_timestamp(*value, buffer),
        }
    }

    fn visit_bytes(&self, value: &'a [u8], buffer: &'a mut Self::Output) {
        *buffer += 4 + (value.len() as i32);
    }

    fn visit_i32(&self, _value: i32, buffer: &'a mut Self::Output) {
        *buffer += 4;
    }

    fn visit_i64(&self, _value: i64, buffer: &'a mut Self::Output) {
        *buffer += 8;
    }

    fn visit_f32(&self, _value: f32, buffer: &'a mut Self::Output) {
        *buffer += 4;
    }

    fn visit_f64(&self, _value: f64, buffer: &'a mut Self::Output) {
        *buffer += 8;
    }

    fn visit_bool(&self, _value: bool, buffer: &'a mut Self::Output) {
        *buffer += 1;
    }

    fn visit_str(&self, value: &'a str, buffer: &'a mut Self::Output) {
        *buffer += 4 + (value.len() as i32);
    }

    fn visit_uuid(&self, _value: Uuid, buffer: &'a mut Self::Output) {
        *buffer += 16;
    }

    fn visit_timestamp(&self, _value: Timestamp, buffer: &'a mut Self::Output) {
        *buffer += 12;
    }

    fn visit_null(&self, _buffer: &'a mut Self::Output) {
        ()
    }
}

pub struct Encoder;

impl<'a> MessageVisitor<'a> for Encoder {
    type Output = BytesMut;

    fn visit_message(&self, message: &Message, buffer: &'a mut BytesMut) {
        let mut flags = util::Flags::empty();

        if let Some(_) = message.timestamp() {
            flags.insert(util::Flags::HAS_TIMESTAMP);
        }

        if let Some(_) = message.expiration() {
            flags.insert(util::Flags::HAS_EXPIRATION);
        }

        if let Some(_) = message.correlation_id() {
            flags.insert(util::Flags::HAS_CORRELATION_ID);
        }

        if message.headers().len() > 0 {
            flags.insert(util::Flags::HAS_HEADERS);
        }

        if let Some(_) = message.body() {
            flags.insert(util::Flags::HAS_BODY);
        }

        buffer.put_i32::<BigEndian>(flags.bits());

        if let Some(timestamp) = message.timestamp() {
            self.visit_timestamp(timestamp, buffer);
        }

        if let Some(expiration) = message.expiration() {
            self.visit_timestamp(expiration, buffer);
        }

        if let Some(correlation_id) = message.correlation_id() {
            self.visit_uuid(correlation_id, buffer);
        }

        if message.headers().len() > 0 {
            self.visit_map(&message.headers(), buffer);
        }

        if let Some(body) = message.body() {
            self.visit_value(body, buffer);
        }
    }

    fn visit_map(&self, map: &Map, buffer: &'a mut BytesMut) {
        buffer.put_i32::<BigEndian>(map.len() as i32);
        for (key, value) in map.iter() {
            self.visit_key(key, buffer);
            self.visit_value(value, buffer);
        }
    }

    fn visit_list(&self, list: &List, buffer: &'a mut BytesMut) {
        buffer.put_i32::<BigEndian>(list.len() as i32);
        for value in list.iter() {
            self.visit_value(value, buffer);
        }
    }

    fn visit_key(&self, key: &Key, buffer: &'a mut BytesMut) {
        match key {
            Key::Str(ref key) => self.visit_str(key, buffer),
            Key::I32(key) => self.visit_i32(*key, buffer),
        }
    }

    fn visit_value(&self, value: &Value, buffer: &'a mut BytesMut) {
        match value {
            Value::Null => buffer.put_u8(0),
            Value::Str(ref value) => {
                buffer.put_u8(1);
                self.visit_str(value, buffer)
            }
            Value::I32(value) => {
                buffer.put_u8(2);
                self.visit_i32(*value, buffer)
            }
            Value::I64(value) => {
                buffer.put_u8(3);
                self.visit_i64(*value, buffer)
            }
            Value::F32(value) => {
                buffer.put_u8(4);
                self.visit_f32(*value, buffer)
            }
            Value::F64(value) => {
                buffer.put_u8(5);
                self.visit_f64(*value, buffer)
            }
            Value::Bool(value) => {
                buffer.put_u8(6);
                self.visit_bool(*value, buffer)
            }
            Value::Bytes(ref value) => {
                buffer.put_u8(7);
                self.visit_bytes(value, buffer)
            }
            Value::Map(ref value) => {
                buffer.put_u8(8);
                self.visit_map(value, buffer)
            }
            Value::List(ref value) => {
                buffer.put_u8(9);
                self.visit_list(value, buffer)
            }
            Value::Uuid(value) => {
                buffer.put_u8(10);
                self.visit_uuid(*value, buffer)
            }
            Value::Timestamp(value) => {
                buffer.put_u8(11);
                self.visit_timestamp(*value, buffer)
            }
        }
    }

    fn visit_bytes(&self, value: &[u8], buffer: &'a mut BytesMut) {
        buffer.put_u32::<BigEndian>(value.len() as u32);
        buffer.put_slice(value);
    }

    fn visit_i32(&self, value: i32, buffer: &'a mut BytesMut) {
        buffer.put_i32::<BigEndian>(value);
    }

    fn visit_i64(&self, value: i64, buffer: &'a mut BytesMut) {
        buffer.put_i64::<BigEndian>(value);
    }

    fn visit_f32(&self, value: f32, buffer: &'a mut BytesMut) {
        buffer.put_f32::<BigEndian>(value);
    }

    fn visit_f64(&self, value: f64, buffer: &'a mut BytesMut) {
        buffer.put_f64::<BigEndian>(value);
    }

    fn visit_bool(&self, value: bool, buffer: &'a mut BytesMut) {
        buffer.put_u8(if value { 1 } else { 0 })
    }

    fn visit_str(&self, value: &'a str, buffer: &'a mut BytesMut) {
        buffer.put_u32::<BigEndian>(value.len() as u32);
        buffer.put_slice(value.as_bytes());
    }

    fn visit_uuid(&self, value: Uuid, buffer: &'a mut BytesMut) {
        buffer.put_slice(value.as_bytes());
    }

    fn visit_timestamp(&self, value: Timestamp, buffer: &'a mut BytesMut) {
        buffer.put_i64::<BigEndian>(value.timestamp());
        buffer.put_i32::<BigEndian>(value.timestamp_subsec_millis() as i32);
    }

    fn visit_null(&self, _buffer: &'a mut BytesMut) {
        ()
    }
}

pub struct Decoder;

impl Decoder {
    pub fn decode_message<'a>(&self, cursor: &'a ZeroCursor<'a>) -> Message<'a> {
        let mut message = Message::new();
        let _version = cursor.get_i32();

        let flags = util::Flags::from_bits(cursor.get_i32()).expect("Error reading flags");

        if flags.contains(util::Flags::HAS_TIMESTAMP) {
            message.set_timestamp(Some(cursor.get_timestamp()));
        }

        if flags.contains(util::Flags::HAS_EXPIRATION) {
            message.set_expiration(Some(cursor.get_timestamp()));
        }

        if flags.contains(util::Flags::HAS_CORRELATION_ID) {
            message.set_correlation_id(Some(cursor.get_uuid()));
        }

        if flags.contains(util::Flags::HAS_HEADERS) {
            let count = cursor.get_i32();
            for _ in 0..count {
                message.headers_mut().insert(self.decode_key(cursor), self.decode_value(cursor));
            }
        }

        if flags.contains(util::Flags::HAS_BODY) {
            message.set_body(Some(self.decode_value(cursor)));
        }

        message
    }

    pub fn decode_key<'a>(&self, cursor: &'a ZeroCursor<'a>) -> Key<'a> {
        let key_type = cursor.get_u8();
        match key_type {
            0 => Key::Str(self.decode_str(cursor).into()),
            1 => Key::I32(cursor.get_i32()),
            _ => panic!("Unsupported key type '{}", key_type),
        }
    }

    pub fn decode_value<'a>(&self, cursor: &'a ZeroCursor<'a>) -> Value<'a> {
        let value_type = cursor.get_u8();
        match value_type {
            0 => Value::Null,
            1 => Value::Str(self.decode_str(cursor).into()),
            2 => Value::I32(cursor.get_i32()),
            3 => Value::I64(cursor.get_i64()),
            4 => Value::F32(cursor.get_f32()),
            5 => Value::F64(cursor.get_f64()),
            6 => Value::Bool(cursor.get_bool()),
            7 => Value::Bytes(self.decode_bytes(cursor).into()),
            8 => Value::List(self.decode_list(cursor)),
            9 => Value::Map(self.decode_map(cursor)),
            10 => Value::Uuid(cursor.get_uuid()),
            11 => Value::Timestamp(cursor.get_timestamp()),
            _ => panic!("Unsupported value type '{}'", value_type),
        }
    }

    fn decode_map<'a>(&self, cursor: &'a ZeroCursor<'a>) -> Map<'a> {
        let mut map = Map::new();
        let count = cursor.get_i32();
        for _ in 0..count {
            map.insert(self.decode_key(cursor), self.decode_value(cursor))
        }
        map
    }

    fn decode_list<'a>(&self, cursor: &'a ZeroCursor<'a>) -> List<'a> {
        let mut list = List::new();
        let count = cursor.get_i32();
        for _ in 0..count {
            list.push(self.decode_value(cursor));
        }
        list
    }

    fn decode_str<'a>(&self, cursor: &'a ZeroCursor<'a>) -> &'a str {
        let size = cursor.get_i32();
        cursor.get_str(size as usize)
    }

    fn decode_bytes<'a>(&self, cursor: &'a ZeroCursor<'a>) -> &'a [u8] {
        let size = cursor.get_i32();
        cursor.get_bytes(size as usize)
    }
}

pub struct ZeroCursor<'a> {
    buffer: &'a [u8],
    position: Cell<usize>,
}

impl<'a> ZeroCursor<'a> {
    pub fn new<T: AsRef<[u8]>>(buffer: &'a T) -> ZeroCursor<'a> {
        ZeroCursor { buffer: buffer.as_ref(), position: Cell::new(0) }
    }

    pub fn get_i32(&'a self) -> i32 {
        Cursor::new(self.get_bytes(4)).get_i32::<BigEndian>()
    }

    pub fn get_i64(&'a self) -> i64 {
        Cursor::new(self.get_bytes(8)).get_i64::<BigEndian>()
    }

    pub fn get_f32(&'a self) -> f32 {
        Cursor::new(self.get_bytes(4)).get_f32::<BigEndian>()
    }

    pub fn get_f64(&'a self) -> f64 {
        Cursor::new(self.get_bytes(8)).get_f64::<BigEndian>()
    }

    pub fn get_u8(&'a self) -> u8 {
        Cursor::new(self.get_bytes(1)).get_u8()
    }

    pub fn get_bool(&'a self) -> bool {
        match self.get_u8() {
            0 => false,
            _ => true,
        }
    }

    pub fn get_str(&'a self, size: usize) -> &'a str {
        str::from_utf8(&self.get_bytes(size as usize)).unwrap()
    }

    fn get_bytes(&'a self, size: usize) -> &'a [u8] {
        let (start, end) = self.advance(size);
        &self.buffer[start..end]
    }

    fn get_timestamp(&'a self) -> Timestamp {
        let mut cursor = Cursor::new(self.get_bytes(12));
        UTC.timestamp(
            cursor.get_i64::<BigEndian>(),
            cursor.get_i32::<BigEndian>() as u32,
        )
    }

    fn get_uuid(&'a self) -> Uuid {
        Uuid::from_bytes(self.get_bytes(16)).unwrap()
    }

    pub fn len(&'a self) -> usize {
        self.buffer.len()
    }

    pub fn position(&'a self) -> usize {
        self.position.get()
    }

    fn advance(&'a self, size: usize) -> (usize, usize) {
        let pos = self.position.get();
        self.position.set(pos + size);
        (pos, pos + size)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calculate_message_size_for_empty_message() {
        let message = Message::new();
        let size = calculate_message_size(&message);
        assert_eq!(size, 4, "Expecting message size of {}", 8);
    }

    #[test]
    fn calculate_message_size_with_string_body() {
        let mut message = Message::new();
        message.set_body(Some("hello"));
        let size = calculate_message_size(&message);
        assert_eq!(size, 4 + 5 + 5);
    }

    #[test]
    fn calculate_message_size_with_headers() {
        let mut message = Message::new();
        message.headers_mut().insert("key1", "value1");
        message.headers_mut().insert("key2", "value2");
        let size = calculate_message_size(&message);
        assert_eq!(size, 4 + 4 + 5 + 4 + 5 + 6 + 5 + 4 + 5 + 6);
    }

    #[test]
    fn calculate_value_sizes() {
        assert_eq!(calculate_value_size(&Value::from("string")), 11);
        assert_eq!(calculate_value_size(&Value::from(32i32)), 5);
        assert_eq!(calculate_value_size(&Value::from(64i64)), 9);
        assert_eq!(calculate_value_size(&Value::from(32.32f32)), 5);
        assert_eq!(calculate_value_size(&Value::from(64.64f64)), 9);
        assert_eq!(calculate_value_size(&Value::from(true)), 2);
    }

    #[test]
    fn calculate_key_sizes() {
        assert_eq!(calculate_key_size(&Key::from("string")), 11);
        assert_eq!(calculate_key_size(&Key::from(32i32)), 5);
    }

    #[test]
    fn test_bytes_to_str() {
        let mut bytes_mut = BytesMut::with_capacity(1024);
        bytes_mut.put("Hello World".as_bytes());

        let bytes = bytes_mut.freeze();
        let bytes_slice: &[u8] = bytes.as_ref();

        println!("{:?}, {:?}", str::from_utf8(&bytes_slice[..5]).unwrap(), str::from_utf8(&bytes_slice[6..]).unwrap());
    }

    #[test]
    fn test_decoder() {
        let mut input = Message::new();
        input.set_timestamp(Some(UTC::now()));
        input.set_expiration(Some(UTC::now()));
        input.headers_mut().insert(Key::from("key"), Value::from("value"));
        input.set_body(Some(Value::from("body")));

        let size = calculate_message_size(&input);
        let mut buffer = BytesMut::with_capacity(size as usize);

        let encoder = Encoder;

        encoder.visit_message(&input, &mut buffer);

        let bytes = buffer.freeze();


        let cursor = ZeroCursor::new(&bytes);

        let decoder = Decoder;

        let output = decoder.decode_message(&cursor);

    }

}
