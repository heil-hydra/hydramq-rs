use std::fs::{self, OpenOptions, File};
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
    directory: PathBuf,
    dat: File,
    idx: File,
    size: u32,
}

pub trait Segment {
    fn write(&mut self, message: &Message);

    fn size(&self) -> u32;
}

impl FileSegment {
    pub fn with_directory<P>(directory: P) -> FileSegment where P: Into<PathBuf> {
        let directory = directory.into();
        fs::create_dir_all(directory.as_path()).expect("Error creating segment directory");

        let mut dat = directory.clone();
        dat.push("segment.dat");
        let dat = dat.as_path();
        let dat = OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(dat.clone())
            .expect(format!("Error creating {:?}", dat).as_str());

        let mut idx = directory.clone();
        idx.push("segment.idx");
        let idx = idx.as_path();
        let mut idx = OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(idx.clone())
            .expect(format!("Error creating {:?}", idx).as_str());
        let file_size = idx.seek((SeekFrom::End(0))).unwrap() as u32;
        eprintln!("file_size = {:?}", file_size);
        let size = (idx.seek(SeekFrom::End(0)).unwrap() as u32) / 4;
        eprintln!("size = {:?}", size);
        FileSegment { directory, dat, idx, size}
    }

    pub fn directory(&self) -> &Path {
        self.directory.as_ref()
    }

    pub fn delete(self) -> io::Result<()> {
        drop(self.dat);
        drop(self.idx);
        fs::remove_dir_all(self.directory)
    }
}

impl Segment for FileSegment {
    fn write(&mut self, message: &Message) {
        let mut header = BytesMut::with_capacity(4);
        let mut contents = BytesMut::with_capacity(200);
        let message_start = self.dat.seek(SeekFrom::End(0)).unwrap();
        encode_message(message, &mut contents);
        header.put_u32::<LittleEndian>(contents.len() as u32);
        let header = header.freeze();
        let contents = contents.freeze();
        self.dat.write_all(header.as_ref()).unwrap();
        self.dat.write_all(contents.as_ref()).unwrap();
        self.idx.seek(SeekFrom::End(0)).unwrap();
        let mut message_start_buffer = BytesMut::with_capacity(4);
        message_start_buffer.put_u32::<LittleEndian>(message_start as u32);
        self.idx.write_all(&mut message_start_buffer);
        self.size += 1;
    }

    fn size(&self) -> u32 {
        self.size
    }
}

#[cfg(test)]
mod test {
    use super::*;

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
    fn size_of_empty_segment() {
        let segment = FileSegment::with_directory("segment2");
        assert_eq!(segment.size(), 0);
        segment.delete().unwrap();
    }

    #[test]
    fn size_of_segment_with_messages() {
        let mut segment = FileSegment::with_directory("segment3");
        let message = Message::new().build();
        &segment.write(&message);
        assert_eq!(segment.size(), 1);
        &segment.write(&message);
        assert_eq!(segment.size(), 2);
        segment.delete().unwrap();
    }

    #[test]
    fn size_of_existng_segment() {
        let mut segment = FileSegment::with_directory("segment4");
        let message = Message::new().build();
        &segment.write(&message);
        assert_eq!(segment.size(), 1);
        &segment.write(&message);
        assert_eq!(segment.size(), 2);
        drop(segment);
        let mut segment = FileSegment::with_directory("segment4");
        assert_eq!(segment.size(), 2);
        segment.delete().unwrap();
    }

    #[test]
    fn write_single_message() {
        let mut segment = FileSegment::with_directory(PathBuf::from("segment5"));
        let message = Message::new()
            .with_body("Hello, World")
            .build();
        segment.write(&message);
        drop(segment);
        let mut dat = OpenOptions::new()
            .read(true)
            .create(false)
            .open("segment/segment.dat").unwrap();
        let seek = dat.seek(SeekFrom::Current(0)).unwrap();

        let mut buffer = [0u8; 4];
        use std::io::Read;
        dat.read_exact(&mut buffer[..]).unwrap();
        let mut bytes = ::bytes::Bytes::from(&buffer[..]).into_buf();

        let message_size = &bytes.get_u32::<LittleEndian>();
        let mut buffer: Vec<u8> = Vec::new();
        dat.read_exact(&mut buffer[..]);
        let mut bytes = buffer.into_buf();
        let output = ::codec::decode_message(&mut bytes);
        assert_eq!(message, output);
        assert_eq!(message.body(), Some(&Value::from("Hello, World!")));
        assert_eq!(message.properties().len(), 0);

        let segment = FileSegment::with_directory(PathBuf::from("segment5"));
        segment.delete();
    }
}