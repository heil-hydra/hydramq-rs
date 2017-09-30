extern crate hydramq;

use hydramq::topic::{FileSegment};

use std::time::{Instant};

fn main() {

    let segment = FileSegment::with_directory("example");
    eprintln!("segment.directory() = {:?}", segment.directory());

    let now = Instant::now();
    let mut counter = 0u32;
    for (_, _) in segment.iter() {
        counter += 1
    }
    println!("Read {} messages in {:?}", counter, now.elapsed());
}
