#![feature(test)]

extern crate hydramq;
extern crate test;

use hydramq::message::{Message, Map, List};
use hydramq::topic::{FileSegment, Segment};

use test::Bencher;

#[bench]
fn segments_write(b: &mut Bencher) {
    let mut segment = FileSegment::with_directory("segments_writes_bench");
    let message = Message::new()
        .with_body("The quick brown fox jumps over the lazy dog").build();
    
    b.iter(|| {
        segment.write(&message);
    });

    segment.delete();
}

#[bench]
fn segments_read(b: &mut Bencher) {
    let mut segment = FileSegment::with_directory("segments_reads_bench");
    let message = Message::new()
        .with_body("The quick brown fox jumps over the lazy dog").build();
    segment.write(&message);

    b.iter(|| {
        let _ = segment.read(0);
    });

    segment.delete();
}