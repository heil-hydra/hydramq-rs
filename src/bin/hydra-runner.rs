extern crate hydramq;

use hydramq::topic::{Segment, FileSegment};
use hydramq::{Message};
use std::path::PathBuf;


fn main() {

    let mut segment = FileSegment::with_directory(PathBuf::from("sample"));

    for i in 0..1_000_000 {
        let message = Message::with_body(format!("Hello {}\n", i)).build();
        segment.write(&message);
    }
}