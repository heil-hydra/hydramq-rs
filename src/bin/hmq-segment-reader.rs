extern crate hydramq;

use hydramq::topic::{FileSegment, Segment};

use std::time::{Instant};

fn main() {

    let segment = FileSegment::with_directory("example");

    let now = Instant::now();
    for i in 0..segment.size() {
        let _ = segment.read(i);
    }
    println!("Read {} messages in {:?}", segment.size(), now.elapsed());
}