
use std::fs::{self,OpenOptions, File};
use std::path::{Path, PathBuf};
use std::io::{self, Seek, SeekFrom, Write};

use message::Message;
use message::Value;

use bytes::{BytesMut, BufMut, Buf, IntoBuf, LittleEndian};

use ::codec::encode_message;
use ::codec::decode_message;

pub mod segment;

pub struct SegmentNumber(i32);

impl Into<String> for SegmentNumber {
    fn into(self) -> String {
        self.0.to_string()
    }
}

impl From<String> for SegmentNumber {
    fn from(value: String) -> Self {
        SegmentNumber(value.parse().expect("Failed to read segment from value"))
    }
}

pub struct FileSegment {
    dat: File,
    idx: File,
}

pub trait Segment {
    fn write(&mut self, message: &Message);
}

impl FileSegment {

    pub fn named(name: SegmentNumber) -> FileSegment {
        let name: String = name.into();
        Self::with_directory(PathBuf::from(name.as_str()))
    }

    pub fn with_directory(name: PathBuf) -> FileSegment {
        fs::create_dir_all(name.as_path()).expect("Error creating segment directory");

        let mut dat = name.clone();
        dat.push("segment.dat");
        let dat = dat.as_path();
        let dat = OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(dat.clone())
            .expect(format!("Error creating {:?}", dat).as_str());

        let mut idx = name.clone();
        idx.push("segment.idx");
        let idx = idx.as_path();
        let idx = OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(idx.clone())
            .expect(format!("Error creating {:?}", idx).as_str());
        Self::new(dat, idx)
    }

    pub fn new(dat: File, idx: File) -> FileSegment {
        FileSegment { dat, idx}
    }
}

impl Segment for FileSegment {
    fn write(&mut self, message: &Message) {
        let mut header = BytesMut::with_capacity(4);
        let mut contents = BytesMut::with_capacity(200);
        let message_start = self.dat.seek(SeekFrom::End(0)).unwrap();
        encode_message(message, &mut contents);
        header.put_u32::<LittleEndian>(contents.len() as u32);
        let mut header = header.freeze();
        let mut contents = contents.freeze();
        self.dat.write_all(header.as_ref()).unwrap();
        self.dat.write_all(contents.as_ref()).unwrap();
        self.idx.seek(SeekFrom::End(0)).unwrap();
        let mut message_start_buffer = BytesMut::with_capacity(4);
        message_start_buffer.put_u32::<LittleEndian>(message_start as u32);
        self.idx.write_all(&mut message_start_buffer);
    }

}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn create_segment() {
        println!("Running test!");
        let dat_path = "segment.dat";
        let dat = OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(dat_path.clone()).expect("Error creating segment.dat");
        let idx_path = "segment.idx";
        let idx = OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(idx_path.clone()).expect("Error creating segment.idx");
        let mut segment = FileSegment::new(dat, idx);

        for i in 0..1000 {
            let message = Message::new().with_body(format!("Hello{}\n", i)).build();
            segment.write(&message);
        }

        drop(segment);
        fs::remove_file(dat_path).expect("Error deleting segment.dat");
        fs::remove_file(idx_path).expect("Error deleting segment.idx");
    }

    #[test]
    fn create_new2() {
        let segment_dir = "example";
        let _ = FileSegment::with_directory(PathBuf::from(segment_dir));
        let dat_file = Path::new("example/segment.dat");
        let idx_file = Path::new("example/segment.idx");
        assert!(dat_file.exists());
        assert!(idx_file.exists());
        fs::remove_dir_all(segment_dir).expect("Error deleting segment directory");
    }

    #[test]
    fn create_segment2() {
        let mut segment = FileSegment::with_directory(PathBuf::from("segment"));
        let message = Message::new()
            .with_body("Hello, World")
            .build();
        segment.write(&message);
    }
}