extern crate hydramq;
extern crate bytes;

use hydramq::codec::message_codec::{encode_message};
use hydramq::message::message::{MessageBuilder};
use std::io::{BufRead, Write};

fn main() {
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    let stdin = std::io::stdin();
    let  stdin = stdin.lock();

    for line in stdin.lines() {
        if let Ok(line) = line {
            eprint!("Reading line {:?}, len: {}", line, line.len());
            let message = MessageBuilder::new().with_body(line).build();
            let buffer = encode_message(&message);
            eprint!("{:?}", buffer);
            stdout.write(buffer.as_ref()).expect("Error writing to buffer");
        }
    }
}
