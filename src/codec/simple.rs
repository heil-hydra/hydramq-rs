use message::message::{Message, List, Map, Key, Value, Timestamp};
use uuid::Uuid;
use std::borrow::Cow;
use chrono::{UTC, TimeZone};
use codec::util;
use bytes::{BufMut, BytesMut, Buf};

trait MessageDecoder<'a, B> {
    fn decode_message(&self, buffer: &mut B) -> Message<'a>;

    fn decode_key(&self, buffer: &mut B) -> Key<'a>;

    fn decode_value(&self, buffer: &mut B) -> Value<'a>;

    fn decode_map(&self, buffer: &mut B) -> Map<'a>;

    fn decode_list(&self, buffer: &mut B) -> List<'a>;

    fn decode_string(&self, buffer: &mut B) -> Cow<'a, str>;

    fn decode_timestamp(&self, buffer: &mut B) -> Timestamp;

    fn decode_uuid(&self, buffer: &mut B) -> Uuid;

    fn decode_bytes(&self, buffer: &mut B) -> Cow<'a, [u8]>;

    fn decode_i32(&self, buffer: &mut B) -> i32;

    fn decode_i64(&self, buffer: &mut B) -> i64;

    fn decode_f32(&self, buffer: &mut B) -> f32;

    fn decode_f64(&self, buffer: &mut B) -> f64;

    fn decode_bool(&self, buffer: &mut B) -> bool;
}

trait MessageEncoder<'a, B> {
    fn encode_message(&self, value: &Message<'a>, buffer: &mut B);

    fn encode_key(&self, value: &Key<'a>, buffer: &mut B);

    fn encode_value(&self, value: &Value<'a>, buffer: &mut B);

    fn encode_map(&self, value: &Map<'a>, buffer: &mut B);

    fn encode_list(&self, value: &List<'a>, buffer: &mut B);

    fn encode_string(&self, value: &Cow<'a, str>, buffer: &mut B);

    fn encode_timestamp(&self, value: Timestamp, buffer: &mut B);

    fn encode_uuid(&self, value: Uuid, buffer: &mut B);

    fn encode_bytes(&self, value: &Cow<'a, [u8]>, buffer: &mut B);

    fn encode_i32(&self, value: i32, buffer: &mut B);

    fn encode_i64(&self, value: i64, buffer: &mut B);

    fn encode_f32(&self, value: f32, buffer: &mut B);

    fn encode_f64(&self, value: f64, buffer: &mut B);

    fn encode_bool(&self, value: bool, buffer: &mut B);
}

pub struct BinaryMessageCodec;

impl<'a, B> MessageDecoder<'a, B> for BinaryMessageCodec
    where B: Buf
{
    fn decode_message(&self, buffer: &mut B) -> Message<'a> {
        let mut message = Message::new();

        let flags = util::Flags::from_bits(self.decode_i32(buffer)).expect("Error reading flags");

        if flags.contains(util::Flags::HAS_TIMESTAMP) {
            message.set_timestamp(Some(self.decode_timestamp(buffer)));
        }

        if flags.contains(util::Flags::HAS_EXPIRATION) {
            message.set_expiration(Some(self.decode_timestamp(buffer)));
        }

        if flags.contains(util::Flags::HAS_CORRELATION_ID) {
            message.set_correlation_id(Some(self.decode_uuid(buffer)));
        }

        if flags.contains(util::Flags::HAS_HEADERS) {
            let count = self.decode_i32(buffer);
            for _ in 0..count {
                message.headers_mut().insert(self.decode_key(buffer), self.decode_value(buffer));
            }
        }

        if flags.contains(util::Flags::HAS_BODY) {
            message.set_body(Some(self.decode_value(buffer)));
        }

        message
    }

    fn decode_key(&self, buffer: &mut B) -> Key<'a> {
        let key_type = buffer.get_u8();
        match key_type {
            1 => Key::Str(self.decode_string(buffer)),
            2 => Key::I32(self.decode_i32(buffer)),
            _ => panic!("Unsupported key type '{}", key_type),
        }
    }

    fn decode_value(&self, buffer: &mut B) -> Value<'a> {
        let value_type = buffer.get_u8();
        match value_type {
            0 => Value::Null,
            1 => Value::Str(self.decode_string(buffer)),
            2 => Value::I32(self.decode_i32(buffer)),
            3 => Value::I64(self.decode_i64(buffer)),
            4 => Value::F32(self.decode_f32(buffer)),
            5 => Value::F64(self.decode_f64(buffer)),
            6 => Value::Bool(self.decode_bool(buffer)),
            7 => Value::Bytes(self.decode_bytes(buffer)),
            8 => Value::List(self.decode_list(buffer)),
            9 => Value::Map(self.decode_map(buffer)),
            10 => Value::Uuid(self.decode_uuid(buffer)),
            11 => Value::Timestamp(self.decode_timestamp(buffer)),
            _ => panic!("Unsupported value type '{}'", value_type),
        }
    }

    fn decode_map(&self, buffer: &mut B) -> Map<'a> {
        let mut map = Map::new();
        let count = buffer.get_i32_be();
        for _ in 0..count {
            map.insert(self.decode_key(buffer), self.decode_value(buffer))
        }
        map
    }

    fn decode_list(&self, buffer: &mut B) -> List<'a> {
        let mut list = List::new();
        let count = buffer.get_i32_be();
        for _ in 0..count {
            list.push(self.decode_value(buffer));
        }
        list
    }

    fn decode_bytes(&self, buffer: &mut B) -> Cow<'a, [u8]> {
        let len = buffer.get_i32_be() as usize;
        let bytes: Vec<u8> = buffer.take(len).collect();
        bytes.into()
    }

    fn decode_string(&self, buffer: &mut B) -> Cow<'a, str> {
        let len = buffer.get_i32_be() as usize;
        String::from_utf8(buffer.take(len).collect()).unwrap().into()
    }

    fn decode_timestamp(&self, buffer: &mut B) -> Timestamp {
        UTC.timestamp(self.decode_i64(buffer), self.decode_i32(buffer) as u32)
    }

    fn decode_uuid(&self, buffer: &mut B) -> Uuid {
        let bytes: Vec<u8> = buffer.take(16).collect();
        Uuid::from_bytes(&bytes).unwrap()
    }

    fn decode_i32(&self, buffer: &mut B) -> i32 {
        buffer.get_i32_be()
    }

    fn decode_i64(&self, buffer: &mut B) -> i64 {
        buffer.get_i64_be()
    }

    fn decode_f32(&self, buffer: &mut B) -> f32 {
        buffer.get_f32_be()
    }

    fn decode_f64(&self, buffer: &mut B) -> f64 {
        buffer.get_f64_be()
    }

    fn decode_bool(&self, buffer: &mut B) -> bool {
        match buffer.get_u8() {
            0 => false,
            _ => true,
        }
    }
}

impl<'a, B> MessageEncoder<'a, B> for BinaryMessageCodec
    where B: BufMut
{
    fn encode_message(&self, message: &Message<'a>, buffer: &mut B) {
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

        self.encode_i32(flags.bits(), buffer);

        if let Some(timestamp) = message.timestamp() {
            self.encode_timestamp(timestamp, buffer);
        }

        if let Some(expiration) = message.expiration() {
            self.encode_timestamp(expiration, buffer);
        }

        if let Some(correlation_id) = message.correlation_id() {
            self.encode_uuid(correlation_id, buffer);
        }

        if message.headers().len() > 0 {
            self.encode_map(&message.headers(), buffer);
        }

        if let Some(body) = message.body() {
            self.encode_value(body, buffer);
        }
    }

    fn encode_key(&self, key: &Key<'a>, buffer: &mut B) {
        match key {
            Key::Str(ref key) => {
                buffer.put_u8(1);
                self.encode_string(key, buffer);
            },
            Key::I32(key) => {
                buffer.put_u8(2);
                self.encode_i32(*key, buffer);
            },
        }
    }

    fn encode_value(&self, value: &Value<'a>, buffer: &mut B) {
        match value {
            Value::Null => buffer.put_u8(0),
            Value::Str(ref value) => {
                buffer.put_u8(1);
                self.encode_string(value, buffer)
            }
            Value::I32(value) => {
                buffer.put_u8(2);
                self.encode_i32(*value, buffer)
            }
            Value::I64(value) => {
                buffer.put_u8(3);
                self.encode_i64(*value, buffer)
            }
            Value::F32(value) => {
                buffer.put_u8(4);
                self.encode_f32(*value, buffer)
            }
            Value::F64(value) => {
                buffer.put_u8(5);
                self.encode_f64(*value, buffer)
            }
            Value::Bool(value) => {
                buffer.put_u8(6);
                self.encode_bool(*value, buffer)
            }
            Value::Bytes(ref value) => {
                buffer.put_u8(7);
                self.encode_bytes(value, buffer)
            }
            Value::Map(ref value) => {
                buffer.put_u8(8);
                self.encode_map(value, buffer)
            }
            Value::List(ref value) => {
                buffer.put_u8(9);
                self.encode_list(value, buffer)
            }
            Value::Uuid(value) => {
                buffer.put_u8(10);
                self.encode_uuid(*value, buffer)
            }
            Value::Timestamp(value) => {
                buffer.put_u8(11);
                self.encode_timestamp(*value, buffer)
            }
        }
    }

    fn encode_map(&self, map: &Map<'a>, buffer: &mut B) {
        self.encode_i32(map.len() as i32, buffer);
        for (key, value) in map.iter() {
            self.encode_key(key, buffer);
            self.encode_value(value, buffer);
        }
    }

    fn encode_list(&self, list: &List<'a>, buffer: &mut B) {
        self.encode_i32(list.len() as i32, buffer);
        for value in list.iter() {
            self.encode_value(value, buffer);
        }
    }

    fn encode_string(&self, value: &Cow<'a, str>, buffer: &mut B) {
        self.encode_i32(value.len() as i32, buffer);
        buffer.put_slice(value.as_ref().as_bytes());
    }

    fn encode_timestamp(&self, value: Timestamp, buffer: &mut B) {
        self.encode_i64(value.timestamp(), buffer);
        self.encode_i32(value.timestamp_subsec_nanos() as i32, buffer);
    }

    fn encode_uuid(&self, value: Uuid, buffer: &mut B) {
        buffer.put_slice(value.as_bytes());
    }

    fn encode_bytes(&self, value: &Cow<'a, [u8]>, buffer: &mut B) {
        self.encode_i32(value.len() as i32, buffer);
        buffer.put_slice(value.as_ref());
    }

    fn encode_i32(&self, value: i32, buffer: &mut B) {
        buffer.put_i32_be(value);
    }

    fn encode_i64(&self, value: i64, buffer: &mut B) {
        buffer.put_i64_be(value);
    }

    fn encode_f32(&self, value: f32, buffer: &mut B) {
        buffer.put_f32_be(value);
    }

    fn encode_f64(&self, value: f64, buffer: &mut B) {
        buffer.put_f64_be(value);
    }

    fn encode_bool(&self, value: bool, buffer: &mut B) {
        buffer.put_u8(if value { 1 } else { 0 })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use codec::message_codec::calculate_message_size;
    use bytes::{BytesMut, IntoBuf};

    #[test]
    fn codec_empty_message() {
        let mut message = Message::new();
        message.set_timestamp(Some(UTC::now()));
        message.set_expiration(Some(UTC::now()));
        message.headers_mut().insert(Key::from("key"), Value::from("value"));
        message.set_body(Some(Value::from("body")));

        let codec = BinaryMessageCodec;

        let size = calculate_message_size(&message);
        println!("{:?}", size);

        let mut buffer_mut = BytesMut::with_capacity(size as usize);
        codec.encode_message(&message, &mut buffer_mut);
        let mut buf = buffer_mut.freeze().into_buf();
        let output = codec.decode_message(&mut buf);

        assert_eq!(message, output);
        println!("{:?}", message.headers().len());
    }
}