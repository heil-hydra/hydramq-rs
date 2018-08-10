use message::List;
use message::Map;
use message::Message;
use message::Value;

use codec::util;

use bytes::{BufMut, BytesMut};
use uuid::Uuid;

pub struct BinaryMessageEncoder();

impl BinaryMessageEncoder {
    pub fn new() -> BinaryMessageEncoder {
        BinaryMessageEncoder {}
    }

    pub fn encode_message(message: &Message, buffer: &mut BytesMut) {
        BinaryMessageEncoder {}.encode_message(message, buffer);
    }
}

impl MessageEncoder for BinaryMessageEncoder {
    fn encode_int32(&self, value: i32, buffer: &mut BytesMut) {
        buffer.put_i32_be(value);
    }

    fn encode_int64(&self, value: i64, buffer: &mut BytesMut) {
        buffer.put_i64_be(value);
    }

    fn encode_float32(&self, value: f32, buffer: &mut BytesMut) {
        buffer.put_f32_be(value);
    }

    fn encode_float64(&self, value: f64, buffer: &mut BytesMut) {
        buffer.put_f64_be(value);
    }

    fn encode_boolean(&self, value: bool, buffer: &mut BytesMut) {
        buffer.put_u8(if value { 1 } else { 0 })
    }

    fn encode_string(&self, value: &String, buffer: &mut BytesMut) {
        buffer.reserve(value.len() + 4);
        buffer.put_u32_be(value.len() as u32);
        buffer.put_slice(value.as_bytes());
    }

    fn encode_message(&self, message: &Message, buffer: &mut BytesMut) {
        let mut flags = util::Flags::empty();
        if message.properties().len() > 0 {
            flags.insert(util::Flags::HAS_HEADERS);
        }
        if message.body() != None {
            flags.insert(util::Flags::HAS_BODY);
        }
        buffer.reserve(4);
        buffer.put_i32_be(flags.bits());

        if message.properties().len() > 0 {
            self.encode_map(message.properties(), buffer);
        }

        if message.body() != None {
            self.encode_value(message.body().unwrap(), buffer);
        }
    }
    fn encode_value(&self, value: &Value, buffer: &mut BytesMut) {
        buffer.reserve(8);
        match value {
            &Value::Null => buffer.put_u8(0),
            &Value::String(ref value) => {
                buffer.put_u8(1);
                self.encode_string(value, buffer);
            }
            &Value::Int32(value) => {
                buffer.put_u8(2);
                self.encode_int32(value, buffer);
            }
            &Value::Int64(value) => {
                buffer.put_u8(3);
                self.encode_int64(value, buffer);
            }
            &Value::Float32(value) => {
                buffer.put_u8(4);
                self.encode_float32(value, buffer);
            }
            &Value::Float64(value) => {
                buffer.put_u8(5);
                self.encode_float64(value, buffer);
            }
            &Value::Boolean(value) => {
                buffer.put_u8(6);
                self.encode_boolean(value, buffer);
            }
            &Value::Bytes(ref value) => {
                buffer.put_u8(7);
                self.encode_bytes(value, buffer);
            }
            &Value::List(ref value) => {
                buffer.put_u8(8);
                self.encode_list(value, buffer);
            }
            &Value::Map(ref value) => {
                buffer.put_u8(9);
                self.encode_map(value, buffer);
            }
            &Value::Uuid(ref value) => {
                buffer.put_u8(10);
                self.encode_uuid(value, buffer);
            }
        }
    }

    fn encode_map(&self, map: &Map, buffer: &mut BytesMut) {
        buffer.reserve(4);
        buffer.put_u32_be(map.len() as u32);
        for (key, value) in map.iter() {
            self.encode_string(key, buffer);
            self.encode_value(value, buffer);
        }
    }
    fn encode_list(&self, list: &List, buffer: &mut BytesMut) {
        buffer.reserve(4);
        buffer.put_u32_be(list.len() as u32);
        for item in list.iter() {
            self.encode_value(item, buffer);
        }
    }
    fn encode_bytes(&self, value: &Vec<u8>, buffer: &mut BytesMut) {
        buffer.reserve(value.len() + 4);
        buffer.put_u32_be(value.len() as u32);
        buffer.put_slice(value);
    }

    fn encode_uuid(&self, value: &Uuid, buffer: &mut BytesMut) {
        buffer.reserve(16);
        buffer.put_slice(value.as_bytes());
    }
}

trait MessageEncoder {
    fn encode_message(&self, value: &Message, buffer: &mut BytesMut);

    fn encode_map(&self, value: &Map, buffer: &mut BytesMut);

    fn encode_list(&self, value: &List, buffer: &mut BytesMut);

    fn encode_value(&self, value: &Value, buffer: &mut BytesMut);

    fn encode_bytes(&self, value: &Vec<u8>, buffer: &mut BytesMut);

    fn encode_int32(&self, value: i32, buffer: &mut BytesMut);

    fn encode_int64(&self, value: i64, buffer: &mut BytesMut);

    fn encode_float32(&self, value: f32, buffer: &mut BytesMut);

    fn encode_float64(&self, value: f64, buffer: &mut BytesMut);

    fn encode_boolean(&self, _value: bool, _buffer: &mut BytesMut);

    fn encode_string(&self, _value: &String, _buffer: &mut BytesMut);

    fn encode_uuid(&self, value: &Uuid, buffer: &mut BytesMut);

    fn encode_null(&self, _buffer: &mut BytesMut) {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Buf;
    use std::io::Cursor;

    #[test]
    fn binary_encode_empty_message() {
        let mut buffer = BytesMut::with_capacity(100);
        let message = Message::new().build();
        BinaryMessageEncoder::encode_message(&message, &mut buffer);

        let mut bytes = Cursor::new(buffer.freeze());
        assert!(bytes.has_remaining());
        let flags = util::Flags::from_bits(bytes.get_i32_be()).unwrap();
        assert!(flags.is_empty());
        assert!(!bytes.has_remaining());
    }

    #[test]
    fn binary_encode_string_message() {
        let mut buffer = BytesMut::new();
        let message = Message::new().with_body("Hello").build();
        BinaryMessageEncoder::encode_message(&message, &mut buffer);
        assert_eq!(buffer.len(), 14);
        let mut expected_buffer = BytesMut::with_capacity(13);
        expected_buffer.put_i32_be(util::Flags::HAS_BODY.bits());
        let body = "Hello";
        expected_buffer.put_u8(1);
        expected_buffer.put_u32_be(body.len() as u32);
        expected_buffer.put_slice(body.as_bytes());
        assert_eq!(buffer, expected_buffer);
    }

    #[test]
    fn binary_encode_kitchen_sink_properties() {
        let mut buffer = BytesMut::new();
        let message = Message::new()
            .with_property("fname", "Jimmie")
            .with_property("lname", "Fulton")
            .with_property("age", 42)
            .with_property("temp", 96.8)
            .with_property(
                "vehicles",
                List::new().append("Aprilia").append("Infiniti").build(),
            )
            .with_property(
                "siblings",
                Map::new()
                    .insert("brothers", List::new().append("Jason").build())
                    .insert(
                        "sisters",
                        List::new().append("Laura").append("Sariah").build(),
                    )
                    .build(),
            )
            .build();
        BinaryMessageEncoder::encode_message(&message, &mut buffer);
    }
}
