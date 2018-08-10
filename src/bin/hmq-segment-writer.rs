fn main() {
    println!("Hello!");
}

//extern crate hydramq;
//
//use hydramq::message::{Message, Map, List};
//use hydramq::topic::{FileSegment, Segment};
//
//fn main() {
//
//    let segment = FileSegment::with_directory("example");
//    let message = example();
//    for _i in 0..1_000_000 {
//        segment.write(&message);
//    }
//}
//
//fn example() -> Message {
//    Message::new()
//        .with_property("fname", "Jimmie")
//        .with_property("lname", "Fulton")
//        .with_property("age", 42)
//        .with_property("temp", 98.6)
//        .with_property("vehicles", List::new()
//            .append("Aprilia")
//            .append("Infiniti")
//            .build()
//        )
//        .with_property("siblings",
//                       Map::new()
//                           .insert("brothers",
//                                   List::new()
//                                       .append("Jason").build()
//                           )
//                           .insert("sisters",
//                                   List::new()
//                                       .append("Laura")
//                                       .append("Sariah")
//                                       .build()
//                           ).build()
//        ).build()
//}