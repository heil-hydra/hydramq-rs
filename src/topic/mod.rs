use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::io::{self, Seek, SeekFrom, Write};

use message::{Message};

use bytes::{Buf, BufMut, BytesMut, IntoBuf};

use codec::encode_message;
use codec::decode_message;

use std::cell::RefCell;
use std::ops::Range;

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
    dat: RefCell<File>,
    idx: RefCell<File>,
}

pub trait Segment {
    fn write(&self, message: &Message);

    fn read(&self, offset: u32) -> Option<Message>;

    fn size(&self) -> u32;
}

impl FileSegment {
    pub fn with_directory<P>(directory: P) -> FileSegment
    where
        P: Into<PathBuf>,
    {
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
        let idx = OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(idx.clone())
            .expect(format!("Error creating {:?}", idx).as_str());
        let idx = RefCell::new(idx);
        let dat = RefCell::new(dat);
        FileSegment {
            directory,
            dat,
            idx,
        }
    }

    pub fn with_temp_directory() -> FileSegment {
        let directory =
            ::std::env::temp_dir().join(::uuid::Uuid::new_v4().hyphenated().to_string());
        FileSegment::with_directory(directory)
    }

    pub fn directory(&self) -> &Path {
        self.directory.as_ref()
    }

    pub fn delete(self) -> io::Result<()> {
        drop(self.dat);
        drop(self.idx);
        fs::remove_dir_all(self.directory)
    }

    pub fn truncate(&self) -> io::Result<()> {
        self.idx.borrow_mut().set_len(0)?;
        self.dat.borrow_mut().set_len(0)?;
        Ok(())
    }

    pub fn iter(&self) -> FileSegmentIter {
        let range = Range {
            start: 0,
            end: self.size(),
        };
        FileSegmentIter {
            range,
            segment: &self,
        }
    }
}

impl Segment for FileSegment {
    fn write(&self, message: &Message) {
        let mut header = BytesMut::with_capacity(4);
        use message::MessageVisitor;
        let calculator = ::message::BinaryFormatSizeCalculator {};
        let mut size = 0;
        calculator.visit_message(&message, &mut size);
        let mut contents = BytesMut::with_capacity(size);
        let mut dat_borrow = self.dat.borrow_mut();
        let message_start = dat_borrow.seek(SeekFrom::End(0)).unwrap();
        encode_message(message, &mut contents);
        header.put_u32_le(contents.len() as u32);
        let header = header.freeze();
        let contents = contents.freeze();

        dat_borrow.write_all(header.as_ref()).unwrap();
        dat_borrow.write_all(contents.as_ref()).unwrap();
        let mut idx_borrow = self.idx.borrow_mut();
        idx_borrow.seek(SeekFrom::End(0)).unwrap();
        let mut message_start_buffer = BytesMut::with_capacity(4);
        message_start_buffer.put_u32_le(message_start as u32);
        idx_borrow.write_all(&mut message_start_buffer).unwrap();
    }

    fn read(&self, offset: u32) -> Option<Message> {
        if self.size() == 0 || offset > self.size() - 1 {
            return None;
        }
        let mut header = [0u8; 4];
        let mut idx_borrow = self.idx.borrow_mut();
        idx_borrow
            .seek(SeekFrom::Start((offset * 4) as u64))
            .unwrap();
        use std::io::Read;
        idx_borrow.read_exact(&mut header[..]).unwrap();
        let mut header_bytes = ::bytes::Bytes::from(&header[..]).into_buf();
        let message_start = header_bytes.get_u32_le();
        let mut dat_borrow = self.dat.borrow_mut();
        dat_borrow
            .seek(SeekFrom::Start(message_start as u64))
            .unwrap();
        dat_borrow.read_exact(&mut header[..]).unwrap();
        let mut header_bytes = ::bytes::Bytes::from(&header[..]).into_buf();
        let message_size = header_bytes.get_u32_le();
        let mut message_buffer = vec![0u8; message_size as usize];
        dat_borrow.read_exact(&mut message_buffer[..]).unwrap();
        let mut message_bytes = message_buffer.into_buf();
        Some(decode_message(&mut message_bytes))
    }

    fn size(&self) -> u32 {
        self.idx.borrow_mut().seek(SeekFrom::End(0)).unwrap() as u32 / 4
    }
}

pub struct FileSegmentIter<'a> {
    range: ::std::ops::Range<u32>,
    segment: &'a FileSegment,
}

impl<'a> Iterator for FileSegmentIter<'a> {
    type Item = (u32, Message);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(index) = self.range.next() {
            let result = self.segment.read(index);
            return match result {
                Some(message) => Some((index, message)),
                None => None,
            };
        }
        None
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        match self.range.nth(n) {
            Some(index) => {
                let result = self.segment.read(index);
                match result {
                    Some(message) => Some((index, message)),
                    None => None,
                }
            }
            None => None,
        }
    }
    //
    //    fn skip(self, n: usize) -> Skip<Self> where Self: Sized {
    //        unimplemented!()
    //    }
}

impl<'a> DoubleEndedIterator for FileSegmentIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(index) = self.range.next_back() {
            let result = self.segment.read(index);
            return match result {
                Some(message) => Some((index, message)),
                None => None,
            };
        }
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use message::Value;

    #[test]
    fn size_of_empty_segment() {
        let segment = FileSegment::with_temp_directory();
        assert_eq!(segment.size(), 0);
        segment.delete().unwrap();
    }

    #[test]
    fn size_of_segment_with_messages() {
        let segment = FileSegment::with_temp_directory();
        let message = Message::new().build();
        &segment.write(&message);
        assert_eq!(segment.size(), 1);
        &segment.write(&message);
        assert_eq!(segment.size(), 2);
        segment.delete().unwrap();
    }

    #[test]
    fn size_of_existng_segment() {
        let segment = FileSegment::with_temp_directory();
        let path = segment.directory().to_owned();
        let message = Message::new().build();
        &segment.write(&message);
        assert_eq!(segment.size(), 1);
        &segment.write(&message);
        assert_eq!(segment.size(), 2);
        drop(segment);
        let segment = FileSegment::with_directory(path);
        assert_eq!(segment.size(), 2);
        segment.delete().unwrap();
    }

    #[test]
    fn write_single_message() {
        let segment = FileSegment::with_temp_directory();
        let path = segment.directory().to_owned();
        let message = Message::new().with_body("Hello, World").build();
        segment.write(&message);
        drop(segment);
        let mut dat = OpenOptions::new()
            .read(true)
            .create(false)
            .open(path.join("segment.dat"))
            .unwrap();
        let _ = dat.seek(SeekFrom::Current(0)).unwrap();

        let mut buffer = [0u8; 4];
        use std::io::Read;
        dat.read_exact(&mut buffer[..]).unwrap();
        let mut bytes = ::bytes::Bytes::from(&buffer[..]).into_buf();

        let message_size = bytes.get_u32_le();

        let mut buf = vec![0u8; message_size as usize];
        dat.read_exact(&mut buf[..]).unwrap();

        let mut bytes = buf.into_buf();

        let output = ::codec::decode_message(&mut bytes);
        assert_eq!(message, output);
        assert_eq!(message.body(), Some(&Value::from("Hello, World")));
        assert_eq!(message.properties().len(), 0);

        let segment = FileSegment::with_directory(path);
        segment.delete().unwrap();
    }

    #[test]
    fn read_empty_segment() {
        let segment = FileSegment::with_temp_directory();
        assert_eq!(segment.read(0), None);
        segment.delete().unwrap();
    }

    #[test]
    fn read_multiple_from_first_offset() {
        let input = Message::with_body("Hello").build();
        let segment = FileSegment::with_temp_directory();
        //        assert_eq!(segment.size(), 0);
        segment.write(&input);
        segment.write(&input);
        //        assert_eq!(segment.size(), 1);
        let output = segment.read(1);
        assert_ne!(output, None);
        assert_eq!(input, output.unwrap());
        assert_eq!(segment.read(0).unwrap(), segment.read(0).unwrap());
        segment.delete().unwrap();
    }

    #[test]
    fn read_multiple_from_second_offset() {
        let message1 = Message::with_body("Hello").build();
        let segment = FileSegment::with_temp_directory();
        segment.write(&message1);
        let message2 = Message::with_body("World").build();
        segment.write(&message2);
        let output1 = segment.read(0);
        let output2 = segment.read(1);
        assert_ne!(output1, None);
        assert_ne!(output2, None);
        assert_eq!(message1, output1.unwrap());
        assert_eq!(message2, output2.unwrap());
        assert_ne!(segment.read(0).unwrap(), segment.read(1).unwrap());
        segment.delete().unwrap();
    }

    #[test]
    fn with_temp_directory() {
        let segment = FileSegment::with_temp_directory();
        eprintln!("segment.directory() = {:?}", segment.directory());
        let message = Message::with_body("Test").build();
        segment.write(&message);
        segment.delete().unwrap();
    }

    #[test]
    fn iterate_file_segment() {
        let segment = FileSegment::with_temp_directory();
        for i in 0..100 {
            let message = Message::with_body("Hello").with_property("iter", i).build();
            segment.write(&message);
        }

        let mut counter = 0u32;

        for (offset, message) in segment.iter() {
            eprintln!(
                "offset: {}, iter = {:?}",
                offset,
                message.properties().get("iter").unwrap()
            );
            counter += 1;
        }

        assert_eq!(counter, 100);

        segment.delete().unwrap();
    }

    #[test]
    fn iterate_file_segment_in_reverse() {
        let segment = example_segment();
        let mut counter = 100u32;
        for (offset, message) in segment.iter().rev() {
            eprintln!(
                "offset: {}, iter = {:?}",
                offset,
                message.properties().get("iter").unwrap()
            );
            counter -= 1;
        }
        assert_eq!(counter, 0);
        segment.delete().unwrap();
    }

    #[test]
    fn iterate_skip_messages() {
        let segment = example_segment();
        for (index, message) in segment.iter().skip(10).take_while(|&(index, _)| index < 15) {
            eprintln!("index = {}, message = {:?}", index, message);
        }
        segment.delete().unwrap();
    }

    #[test]
    fn iterate_nth_messages() {
        let segment = example_segment();
        let counter = 0;
        if let Some((index, _)) = segment.iter().nth(10) {
            eprintln!("index = {:?}", index);
        }

        eprintln!("counter = {:?}", counter);
        segment.delete().unwrap();
    }

    #[test]
    fn iterate_chain() {
        let segment1 = example_segment();
        let segment2 = example_segment();

        for message in segment1
            .iter()
            .skip(50)
            .chain(segment2.iter())
            .map(|(_, message)| message)
        {
            eprintln!("message = {:?}", message);
        }
    }

    fn example_segment() -> FileSegment {
        let segment = FileSegment::with_temp_directory();
        for i in 0..100 {
            let message = Message::with_body("Hello").with_property("iter", i).build();
            segment.write(&message);
        }
        segment
    }
}
