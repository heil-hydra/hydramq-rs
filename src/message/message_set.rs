use std::slice::Iter;
use ::message::Message;

pub struct MessageSet {
    index: u64,
    messages: Vec<Message>,
}

impl MessageSet {
    pub fn index(&self) -> u64 {
        self.index
    }

    pub fn iter(&self) -> MessageSetIter {
        MessageSetIter { counter: 0, index: self.index, messages: &self.messages }
    }

    pub fn len(&self) -> usize {
        self.messages.len()
    }

    pub fn starting_at(index: u64) -> MessageSetBuilder {
        MessageSetBuilder::starting_at(index)
    }
}

struct MessageSetIter<'a> {
    counter: usize,
    index: u64,
    messages: &'a Vec<Message>,
}

impl<'a> Iterator for MessageSetIter<'a> {
    type Item = (u64, &'a Message);

    fn next(&mut self) -> Option<Self::Item> {
        match self.messages.get(self.counter) {
            Some(message) => {
                let current_counter = self.counter;
                let current_index = self.index;
                self.index += 1;
                self.counter += 1;
                Some((current_index, message))
            }
            None => None,
        }
    }
}

pub struct MessageSetBuilder {
    index: u64,
    messages: Vec<Message>,
}

impl MessageSetBuilder {
    fn starting_at(index: u64) -> MessageSetBuilder {
        MessageSetBuilder { index, messages: Vec::new() }
    }

    fn append(mut self, message: Message) -> MessageSetBuilder {
        self.messages.push(message);
        self
    }

    fn build(self) -> MessageSet {
        MessageSet { index: self.index, messages: self.messages }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_set_add() {
        let mut message_set = MessageSet::starting_at(50)
            .append(Message::with_body("Hello").build())
            .append(Message::with_body("World").build()).build();

        let mut builder2 = MessageSet::starting_at(60);

        for i in 0..10 {
            builder2 = builder2.append(Message::with_property("iter", i).build());
        }

        let mut message_set2 = builder2.build();

        assert_eq!(message_set.len(), 2);

        for (index, message) in message_set.iter().chain(message_set2.iter()) {
            eprintln!("index = {:?}, message: {:?}", index, message);
        }
    }
}