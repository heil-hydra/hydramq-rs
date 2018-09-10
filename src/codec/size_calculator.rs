use message::message::Message;
use message::message::Key;
use message::message::Value;
use message::message::Map;
use message::message::List;
use message::message::Timestamp;
use std::str;
use uuid::Uuid;
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
}
