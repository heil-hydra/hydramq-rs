use bytes::{self, BufMut, IntoBuf};
use std::io::{Cursor, Read};

use ::message::{Message, Value, List, Map};
use ::codec::util::*;

pub struct BinaryMessageDecoder {}

impl BinaryMessageDecoder {
    pub fn decode<B>(bytes: &mut B) -> Message where B: bytes::Buf {
        BinaryMessageDecoder {}.decode_message(bytes)
    }
}

impl MessageDecoder for BinaryMessageDecoder {
    fn decode_message<B>(&self, bytes: &mut B) -> Message where B: bytes::Buf {
        let mut builder = Message::new();
        let flags = Flags::from_bits(bytes.get_u32::<bytes::LittleEndian>()).unwrap();
        if flags.contains(Flags::HAS_PROPERTIES) {
            let property_count = bytes.get_u32::<bytes::LittleEndian>();
            for i in 0..property_count {
                let key = self.decode_string(bytes);
                let value = self.decode_value(bytes);
                builder = builder.with_property(key, value);
            };
        }
        if flags.contains(Flags::HAS_BODY) {
            builder = builder.with_body(self.decode_value(bytes));
        }

        builder.build()
    }

    fn decode_string<B>(&self, bytes: &mut B) -> String where B: bytes::Buf {
        use bytes::Buf;
        let len = bytes.get_u32::<bytes::LittleEndian>();
        let mut value = String::with_capacity(len as usize);
        bytes.take(len as usize).reader().read_to_string(&mut value).unwrap();
        value
    }

    fn decode_value<B>(&self, bytes: &mut B) -> Value where B: bytes::Buf {
        let value_type = bytes.get_u8();
        match value_type {
            0 => Value::Null,
            1 => Value::String(self.decode_string(bytes)),
            2 => Value::Int32(self.decode_i32(bytes)),
            3 => Value::Int64(self.decode_i64(bytes)),
            4 => Value::Float64(self.decode_f64(bytes)),
            5 => Value::Boolean(self.decode_bool(bytes)),
            7 => Value::Map(self.decode_map(bytes)),
            8 => Value::List(self.decode_list(bytes)),
            _ => panic!("Unsupported value type '{}'", value_type),
        }
    }
    fn decode_i32<B>(&self, bytes: &mut B) -> i32 where B: bytes::Buf {
        bytes.get_i32::<bytes::LittleEndian>()
    }

    fn decode_i64<B>(&self, bytes: &mut B) -> i64 where B: bytes::Buf {
        bytes.get_i64::<bytes::LittleEndian>()
    }

    fn decode_f64<B>(&self, bytes: &mut B) -> f64 where B: bytes::Buf {
        bytes.get_f64::<bytes::LittleEndian>()
    }

    fn decode_bool<B>(&self, bytes: &mut B) -> bool where B: bytes::Buf {
        match bytes.get_u8() {
            0 => false,
            _ => true,
        }
    }
    fn decode_list<B>(&self, bytes: &mut B) -> List where B: bytes::Buf {
        let mut builder = List::new();
        let item_count = bytes.get_u32::<bytes::LittleEndian>();
        for _ in 0..item_count {
            builder = builder.append(self.decode_value(bytes));
        };
        builder.build()
    }

    fn decode_map<B>(&self, bytes: &mut B) -> Map where B: bytes::Buf {
        let mut builder = Map::new();
        let item_count = bytes.get_u32::<bytes::LittleEndian>();
        for _ in 0..item_count {
            builder = builder.insert(self.decode_string(bytes), self.decode_value(bytes));
        };
        builder.build()
    }
}

trait MessageDecoder {
    fn decode_message<B>(&self, bytes: &mut B) -> Message where B: bytes::Buf;

    fn decode_list<B>(&self, bytes: &mut B) -> List where B: bytes::Buf;

    fn decode_map<B>(&self, bytes: &mut B) -> Map where B: bytes::Buf;

    fn decode_value<B>(&self, bytes: &mut B) -> Value where B: bytes::Buf;

    fn decode_string<B>(&self, bytes: &mut B) -> String where B: bytes::Buf;

    fn decode_i32<B>(&self, bytes: &mut B) -> i32 where B: bytes::Buf;

    fn decode_i64<B>(&self, bytes: &mut B) -> i64 where B: bytes::Buf;

    fn decode_f64<B>(&self, bytes: &mut B) -> f64 where B: bytes::Buf;

    fn decode_bool<B>(&self, bytes: &mut B) -> bool where B: bytes::Buf;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_length_prefixed_string() {
        let mut buffer = bytes::BytesMut::with_capacity(12);
        buffer.put_u32::<bytes::LittleEndian>(5);
        buffer.put_slice("Hello".as_ref());
        buffer.put_u32::<bytes::LittleEndian>(5);
        buffer.put_slice("World".as_ref());

        let mut bytes = buffer.freeze().into_buf();

        let decoder = BinaryMessageDecoder {};

        assert_eq!("Hello", decoder.decode_string(&mut bytes));
        assert_eq!("World", decoder.decode_string(&mut bytes));
    }

    #[test]
    fn decode_string_body() {
        let mut buffer = bytes::BytesMut::with_capacity(100);
        buffer.put_u32::<bytes::LittleEndian>(Flags::HAS_BODY.bits());
        buffer.put_u8(1);
        buffer.put_u32::<bytes::LittleEndian>(5);
        buffer.put_slice("Hello".as_ref());

        let message = decode(buffer);
        assert_eq!(message.body(), Some(&Value::from("Hello")));
        assert_eq!(message.properties().len(), 0);
    }

    #[test]
    fn decode_single_property() {
        let message = Message::with_property("f", 9).build();
        let buffer = encode(&message);
        eprintln!("buffer = {:?}", buffer);
        let output = encode_decode(&message);
    }

    #[test]
    fn decode_kitchen_sink() {
        let input = Message::new()
            .with_property("fname", "Jimmie")
            .with_property("lname", "Fulton")
            .with_property("age", 42)
            .with_property("temp", 96.8)
            .with_property("vehicles", List::new()
                .append("Aprilia")
                .append("Infiniti")
                .build()
            )
            .with_property("siblings",
                           Map::new()
                               .insert("brothers",
                                       List::new()
                                           .append("Jason").build()
                               )
                               .insert("sisters",
                                       List::new()
                                           .append("Laura")
                                           .append("Sariah")
                                           .build()
                               ).build()
            ).build();

        let output = encode_decode(&input);
        assert_eq!(input, output);
    }

    #[test]
    fn test_speed() {
        let message = example();
        for _ in 0..10_000 {
            let output = encode_decode(&message);
        }
    }

    fn encode_decode(message: &Message) -> Message {
        let mut buffer = encode(message);
        decode(buffer)
    }

    fn encode(message: &Message) -> bytes::BytesMut {
        let mut buffer = bytes::BytesMut::new();
        ::codec::encoder::BinaryMessageEncoder::encode(&message, &mut buffer);
        buffer
    }

    fn decode(buffer: bytes::BytesMut) -> Message {
        let mut bytes = buffer.freeze().into_buf();
        BinaryMessageDecoder::decode(&mut bytes)
    }

    fn example() -> Message {
        Message::new()
            .with_property("fname", "Jimmie")
            .with_property("lname", "Fulton")
            .with_property("age", 42)
            .with_property("temp", 96.8)
            .with_property("vehicles", List::new()
                .append("Aprilia")
                .append("Infiniti")
                .build()
            )
            .with_property("siblings",
                           Map::new()
                               .insert("brothers",
                                       List::new()
                                           .append("Jason").build()
                               )
                               .insert("sisters",
                                       List::new()
                                           .append("Laura")
                                           .append("Sariah")
                                           .build()
                               ).build()
            ).build()
    }
}
