extern crate hydramq;
extern crate bytes;
extern crate byteorder;

use hydramq::codec::message_codec::{decode_message};
use bytes::BytesMut;
use hydramq::message::message::Value;
use std::io::{Read, Write};
use bytes::BigEndian;
use byteorder::ReadBytesExt;

fn main() {
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    let stdin = std::io::stdin();
    let mut stdin = stdin.lock();

    while let Ok(len) = stdin.read_i32::<BigEndian>() {
        eprintln!("{:?}", len);
        let mut buffer = vec![0u8; len as usize];

        stdin.read_exact(buffer.as_mut_slice());
        eprintln!("{:?}", buffer);
        let message = decode_message(buffer);
        if let Some(Value::Str(line)) = message.body() {
            eprintln!("Printing message body");
            stdout.write(line.as_bytes());
            stdout.write(b"\n");
        }
    }
}