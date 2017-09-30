#![feature(test)]

extern crate hydramq;
extern crate test;

use hydramq::message::{Message, List, Map, MessageVisitor};
use hydramq::topic::{FileSegment, Segment};

use test::Bencher;

#[bench]
fn segments_write(b: &mut Bencher) {
    let segment = FileSegment::with_directory("segments_writes_bench");
    let message = example();

    b.iter(|| {
        segment.write(&message);
    });

    segment.delete().unwrap();
}

#[bench]
fn segments_read(b: &mut Bencher) {
    let segment = FileSegment::with_directory("segments_reads_bench");
    let message = example();
    segment.write(&message);

    b.iter(|| {
        let _ = segment.read(0);
    });

    segment.delete().unwrap();
}

#[bench]
fn segments_iter(b: &mut Bencher) {
    let segment = FileSegment::with_directory("segments_reads_bench");
    for _ in 0..500_000 {
        let message = Message::new()
            .with_body("The quick brown fox jumps over the lazy dog").build();
        segment.write(&message);
    }

    {
        let mut iter = segment.iter();

        b.iter(|| {
            let _ = &iter.next();
        });
    }

    segment.delete().unwrap();
}

#[bench]
fn message_bytes_calculator(b: &mut Bencher) {
    let message = example();
    let calculator = hydramq::message::BinaryFormatSizeCalculator{};

    b.iter(|| {
        let mut size = 0;
        calculator.visit_message(&message, &mut size);
    });
}

fn example() -> Message {
    Message::new()
        .with_property("fname", "Jimmie")
        .with_property("lname", "Fulton")
        .with_property("age", 42)
        .with_property("temp", 98.6)
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