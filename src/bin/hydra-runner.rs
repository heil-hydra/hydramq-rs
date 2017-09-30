extern crate hydramq;
extern crate bytes;

use bytes::{IntoBuf};

use hydramq::message::{Message, List, Map};

fn main() {

    let message = example();

    for _ in 0..1_000_000 {
        let _ = encode_decode(&message);
    }
}

fn encode_decode(message: &Message) -> Message {
    let buffer = encode(message);
    decode(buffer)
}

fn encode(message: &Message) -> bytes::BytesMut {
    let mut size = 0;
    let mut buffer = bytes::BytesMut::with_capacity(300);
    hydramq::codec::encode_message(&message, &mut buffer);
    buffer
}

fn decode(buffer: bytes::BytesMut) -> Message {
    let mut bytes = buffer.freeze().into_buf();
    hydramq::codec::decode_message(&mut bytes)
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