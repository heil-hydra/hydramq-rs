mod message_set;

use std;
use std::fmt;

use linked_hash_map::{Iter, LinkedHashMap};
use uuid::Uuid;

pub mod message;

#[derive(Debug, PartialEq)]
pub struct Message {
    properties: Map,
    body: Option<Value>,
}

impl Message {
    pub fn new() -> MessageBuilder {
        MessageBuilder::new()
    }

    pub fn with_property<K, V>(key: K, value: V) -> MessageBuilder
    where
        K: Into<String>,
        V: Into<Value>,
    {
        MessageBuilder::new().with_property(key.into(), value.into())
    }

    pub fn with_body<V>(value: V) -> MessageBuilder
    where
        V: Into<Value>,
    {
        MessageBuilder::new().with_body(value.into())
    }

    pub fn properties(&self) -> &Map {
        &self.properties
    }

    pub fn body(&self) -> Option<&Value> {
        match self.body {
            Some(ref value) => Some(value),
            None => None,
        }
    }
}

pub struct MessageBuilder {
    map: LinkedHashMap<String, Value>,
    body: Option<Value>,
}

impl MessageBuilder {
    pub fn new() -> MessageBuilder {
        MessageBuilder {
            map: LinkedHashMap::new(),
            body: None,
        }
    }

    pub fn with_property<K, V>(mut self, key: K, value: V) -> MessageBuilder
    where
        K: Into<String>,
        V: Into<Value>,
    {
        self.map.insert(key.into(), value.into());
        self
    }

    pub fn with_body<V>(mut self, value: V) -> MessageBuilder
    where
        V: Into<Value>,
    {
        self.body = Some(value.into());
        self
    }

    pub fn build(self) -> Message {
        Message {
            properties: Map { map: self.map },
            body: self.body,
        }
    }
}

#[derive(PartialEq, Clone)]
pub struct Map {
    map: LinkedHashMap<String, Value>,
}

impl Map {
    pub fn new() -> MapBuilder {
        MapBuilder {
            map: LinkedHashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.map.get(key)
    }

    pub fn iter(&self) -> Iter<String, Value> {
        self.map.iter()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
}

impl fmt::Debug for Map {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.map.fmt(f)
    }
}

pub struct MapBuilder {
    map: LinkedHashMap<String, Value>,
}

impl MapBuilder {
    pub fn insert<K, V>(mut self, key: K, value: V) -> MapBuilder
    where
        K: Into<String>,
        V: Into<Value>,
    {
        self.map.insert(key.into(), value.into());
        self
    }

    pub fn build(self) -> Map {
        Map { map: self.map }
    }
}

#[derive(Clone, PartialEq)]
pub struct List {
    list: Vec<Value>,
}

impl List {
    pub fn new() -> ListBuilder {
        ListBuilder { list: Vec::new() }
    }

    pub fn iter(&self) -> std::slice::Iter<Value> {
        self.list.iter()
    }

    pub fn len(&self) -> usize {
        self.list.len()
    }
}

impl std::ops::Index<usize> for List {
    type Output = Value;

    fn index(&self, index: usize) -> &Self::Output {
        &self.list[index]
    }
}

impl fmt::Debug for List {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.list.fmt(f)
    }
}

pub struct ListBuilder {
    list: Vec<Value>,
}

impl ListBuilder {
    pub fn append<V>(mut self, value: V) -> ListBuilder
    where
        V: Into<Value>,
    {
        self.list.push(value.into());
        self
    }

    pub fn build(self) -> List {
        List { list: self.list }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    Null,
    String(String),
    Int64(i64),
    Int32(i32),
    Float32(f32),
    Float64(f64),
    Boolean(bool),
    Bytes(Vec<u8>),
    List(List),
    Map(Map),
    Uuid(Uuid),
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value)
    }
}

impl<'a> From<&'a str> for Value {
    fn from(value: &'a str) -> Self {
        Value::String(value.to_string())
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Int64(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Value::Int32(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Float64(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Boolean(value)
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Value::Bytes(value)
    }
}

impl From<List> for Value {
    fn from(value: List) -> Self {
        Value::List(value)
    }
}

impl From<Map> for Value {
    fn from(value: Map) -> Self {
        Value::Map(value)
    }
}

impl From<Uuid> for Value {
    fn from(value: Uuid) -> Self {
        Value::Uuid(value)
    }
}

pub trait MessageVisitor {
    type Output;

    fn visit_message(&self, value: &Message, buffer: &mut Self::Output);

    fn visit_map(&self, value: &Map, buffer: &mut Self::Output);

    fn visit_list(&self, value: &List, buffer: &mut Self::Output);

    fn visit_value(&self, value: &Value, buffer: &mut Self::Output);

    fn visit_bytes(&self, value: &Vec<u8>, buffer: &mut Self::Output);

    fn visit_int32(&self, value: i32, buffer: &mut Self::Output);

    fn visit_int64(&self, value: i64, buffer: &mut Self::Output);

    fn visit_float32(&self, value: f32, buffer: &mut Self::Output);

    fn visit_float64(&self, value: f64, buffer: &mut Self::Output);

    fn visit_boolean(&self, _value: bool, _buffer: &mut Self::Output);

    fn visit_string(&self, _value: &String, _buffer: &mut Self::Output);

    fn visit_uuid(&self, value: &Uuid, buffer: &mut Self::Output);

    fn visit_null(&self, _buffer: &mut Self::Output);
}

pub struct BinaryFormatSizeCalculator {}

impl MessageVisitor for BinaryFormatSizeCalculator {
    type Output = usize;

    fn visit_message(&self, message: &Message, buffer: &mut Self::Output) {
        *buffer += 4;
        for (key, value) in message.properties().iter() {
            self.visit_string(key, buffer);
            self.visit_value(value, buffer);
        }
        if let Some(value) = message.body() {
            self.visit_value(value, buffer);
        }
    }

    fn visit_map(&self, map: &Map, buffer: &mut Self::Output) {
        *buffer += map.len();
        for (key, value) in map.iter() {
            self.visit_string(key, buffer);
            self.visit_value(value, buffer);
        }
    }

    fn visit_list(&self, list: &List, buffer: &mut Self::Output) {
        *buffer += list.len();
        for value in list.iter() {
            self.visit_value(value, buffer);
        }
    }

    fn visit_value(&self, value: &Value, buffer: &mut Self::Output) {
        *buffer += 1;
        match value {
            &Value::Null => self.visit_null(buffer),
            &Value::String(ref value) => {
                self.visit_string(value, buffer);
            }
            &Value::Int32(value) => {
                self.visit_int32(value, buffer);
            }
            &Value::Int64(value) => {
                self.visit_int64(value, buffer);
            }
            &Value::Float32(value) => {
                self.visit_float32(value, buffer);
            }
            &Value::Float64(value) => {
                self.visit_float64(value, buffer);
            }
            &Value::Boolean(value) => {
                self.visit_boolean(value, buffer);
            }
            &Value::Bytes(ref value) => {
                self.visit_bytes(value, buffer);
            }
            &Value::Map(ref value) => {
                self.visit_map(value, buffer);
            }
            &Value::List(ref value) => {
                self.visit_list(value, buffer);
            }
            &Value::Uuid(ref value) => {
                self.visit_uuid(value, buffer);
            }
        }
    }

    fn visit_bytes(&self, value: &Vec<u8>, buffer: &mut Self::Output) {
        *buffer += 4 + value.len()
    }

    fn visit_int32(&self, _value: i32, buffer: &mut Self::Output) {
        *buffer += 4;
    }

    fn visit_int64(&self, _value: i64, buffer: &mut Self::Output) {
        *buffer += 8;
    }

    fn visit_float32(&self, _value: f32, buffer: &mut Self::Output) {
        *buffer += 4;
    }

    fn visit_float64(&self, _value: f64, buffer: &mut Self::Output) {
        *buffer += 8;
    }

    fn visit_boolean(&self, _value: bool, buffer: &mut Self::Output) {
        *buffer += 1;
    }

    fn visit_string(&self, value: &String, buffer: &mut Self::Output) {
        *buffer += 4 + value.len()
    }

    fn visit_uuid(&self, _value: &Uuid, buffer: &mut Self::Output) {
        *buffer += 16
    }

    fn visit_null(&self, _buffer: &mut Self::Output) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let message = Message::new()
            .with_body("Hello")
            .with_property(
                "vehicles",
                List::new().append("Aprilia").append("Infiniti").build(),
            )
            .with_property(
                "address",
                Map::new()
                    .insert("street", "400 Beale ST")
                    .insert("city", "San Francisco")
                    .insert("state", "CA")
                    .insert("zip", "94105")
                    .build(),
            )
            .build();
        println!("message = {:?}", message);
        let message = Message::new()
            .with_body("Wicked!!")
            .with_property("Hello", "World!")
            .with_property("age", 42)
            .with_property("weight", 175.5)
            .with_property("address", "400 Beale ST APT 1403")
            .with_property("city", "San Francisco")
            .with_property("state", "CA")
            .with_property("zip", "94105")
            .with_property("married", false)
            .build();

        println!("message = {:?}", &message);
    }

    #[test]
    fn get_property_value() {
        let m = Message::with_property("msg", "World!").build();
        assert_eq!(m.properties().get("msg"), Some(&Value::from("World!")));
        assert_eq!(m.properties().get("missing"), None);

        if let Some(&Value::String(ref value)) = m.properties().get("msg") {
            println!("value = {:?}", value);
        }

        assert_eq!(m.body(), None);
    }

    #[test]
    fn map_as_body() {
        let m = Message::with_body(
            Map::new()
                .insert("fname", "Jimmie")
                .insert("lname", "Fulton")
                .build(),
        ).build();

        println!("message = {:?}", &m);

        match m.body() {
            Some(&Value::Map(ref map)) => {
                assert_eq!(map.get("fname"), Some(&Value::from("Jimmie")));
                assert_eq!(map.get("lname"), Some(&Value::from("Fulton")));
            }
            _ => panic!("Map expected!"),
        }
    }

    #[test]
    fn list_index() {
        let l = List::new()
            .append("one")
            .append("two")
            .append("three")
            .build();
        assert_eq!(l[0], Value::from("one"));
    }

    #[test]
    fn map_iterator() {
        let map = Map::new()
            .insert("key1", "value1")
            .insert("key2", "value2")
            .build();

        let mut counter = 0;

        for (_key, _value) in map.iter() {
            counter += 1;
        }
        assert_eq!(counter, 2);
        eprintln!("message = {:?}", map);
    }

    #[test]
    pub fn examples() {}

    #[test]
    fn binary_size_calulator() {
        let calculator = BinaryFormatSizeCalculator {};
        let message = Message::with_body("Hello").build();
        let mut size = 0;
        calculator.visit_message(&message, &mut size);
        assert_eq!(size, 14);
    }

    #[test]
    fn binary_size_calcuator_2() {
        let calculator = BinaryFormatSizeCalculator {};
        let message = example();
        let mut size = 0;
        calculator.visit_message(&message, &mut size);
        eprintln!("size = {:?}", size);
    }

    fn example() -> Message {
        Message::new()
            .with_property("fname", "Jimmie")
            .with_property("lname", "Fulton")
            .with_property("age", 42)
            .with_property("temp", 98.6)
            .with_property(
                "vehicles",
                List::new().append("Aprilia").append("Infiniti").build(),
            )
            .with_property(
                "siblings",
                Map::new()
                    .insert("brothers", List::new().append("Jason").build())
                    .insert(
                        "sisters",
                        List::new().append("Laura").append("Sariah").build(),
                    )
                    .build(),
            )
            .build()
    }
}
