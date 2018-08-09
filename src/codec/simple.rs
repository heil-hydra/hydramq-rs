use message::message::{Message, List, Map, Key, Value, Timestamp};
use uuid::Uuid;
use std::borrow::Cow;


pub trait MessageDecoder {
    fn decode_message<'a, B: 'a>(&self, bytes: &mut B) -> Message<'a>
        where
            B: bytes::Buf;

    fn decode_list<'a, B: 'a>(&self, bytes: &mut B) -> List<'a>
        where
            B: bytes::Buf;

    fn decode_map<'a, B: 'a>(&self, bytes: &mut B) -> Map<'a>
        where
            B: bytes::Buf;

    fn decode_key<'a, B: 'a>(&self, bytes: &mut B) -> Key<'a>
        where
            B: bytes::Buf;

    fn decode_value<'a, B: 'a>(&self, bytes: &mut B) -> Value<'a>
        where
            B: bytes::Buf;

    fn decode_string<'a, B: 'a>(&self, bytes: &mut B) -> Cow<'a, str>
        where
            B: bytes::Buf;

    fn decode_i32<'a, B: 'a>(&self, bytes: &mut B) -> i32
        where
            B: bytes::Buf;

    fn decode_i64<'a, B: 'a>(&self, bytes: &mut B) -> i64
        where
            B: bytes::Buf;

    fn decode_f32<'a, B: 'a>(&self, bytes: &mut B) -> f32
        where
            B: bytes::Buf;

    fn decode_f64<'a, B: 'a>(&self, bytes: &mut B) -> f64
        where
            B: bytes::Buf;

    fn decode_bool<'a, B: 'a>(&self, bytes: &mut B) -> bool
        where
            B: bytes::Buf;

    fn decode_uuid<'a, B: 'a>(&self, bytes: &mut B) -> Uuid
        where
            B: bytes::Buf;

    fn decode_bytes<'a, B: 'a>(&self, bytes: &mut B) -> Cow<'a, [u8]>
        where
            B: bytes::Buf;

    fn decode_timestamp<'a, B: 'a>(&self, bytes: &mut B) -> Timestamp
        where B: bytes::Buf;
}

pub struct BinaryMessageDecoder;

impl MessageDecoder for BinaryMessageDecoder {
    fn decode_message<'a, B: 'a>(&self, bytes: &mut B) -> Message<'a> where
        B: bytes::Buf {
        unimplemented!()
    }

    fn decode_list<'a, B: 'a>(&self, bytes: &'_ mut B) -> List<'a> where
        B: bytes::Buf {
        unimplemented!()
    }

    fn decode_map<'a, B: 'a>(&self, bytes: &'_ mut B) -> Map<'a> where
        B: bytes::Buf {
        unimplemented!()
    }

    fn decode_key<'a, B: 'a>(&self, bytes: &'_ mut B) -> Key<'a> where
        B: bytes::Buf {
        unimplemented!()
    }

    fn decode_value<'a, B: 'a>(&self, bytes: &'_ mut B) -> Value<'a> where
        B: bytes::Buf {
        unimplemented!()
    }

    fn decode_string<'a, B: 'a>(&self, bytes: &'_ mut B) -> Cow<'a, str> where
        B: bytes::Buf {
        unimplemented!()
    }

    fn decode_i32<'a, B: 'a>(&self, bytes: &'_ mut B) -> i32 where
        B: bytes::Buf {
        unimplemented!()
    }

    fn decode_i64<'a, B: 'a>(&self, bytes: &'_ mut B) -> i64 where
        B: bytes::Buf {
        unimplemented!()
    }

    fn decode_f32<'a, B: 'a>(&self, bytes: &'_ mut B) -> f32 where
        B: bytes::Buf {
        unimplemented!()
    }

    fn decode_f64<'a, B: 'a>(&self, bytes: &'_ mut B) -> f64 where
        B: bytes::Buf {
        unimplemented!()
    }

    fn decode_bool<'a, B: 'a>(&self, bytes: &'_ mut B) -> bool where
        B: bytes::Buf {
        unimplemented!()
    }

    fn decode_uuid<'a, B: 'a>(&self, bytes: &'_ mut B) -> Uuid where
        B: bytes::Buf {
        unimplemented!()
    }

    fn decode_bytes<'a, B: 'a>(&self, bytes: &'_ mut B) -> Cow<'a, [u8]> where
        B: bytes::Buf {
        unimplemented!()
    }

    fn decode_timestamp<'a, B: 'a>(&self, bytes: &'_ mut B) -> DateTime<UTC> where B: bytes::Buf {
        unimplemented!()
    }
}