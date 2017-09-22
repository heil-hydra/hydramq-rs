extern crate linked_hash_map;

use linked_hash_map::LinkedHashMap;
use std::fmt::{self};

pub mod message;
pub mod topic;

#[derive(Debug)]
pub struct Message {
    properties: Map,
    body: Value,
}
impl Message {
    pub fn new() -> MessageBuilder {
        MessageBuilder::new()
    }

    pub fn with_property<K, V>(key: K, value: V) -> MessageBuilder
        where K: Into<String>, V: Into<Value> {
        MessageBuilder::new().with_property(key.into(), value.into())
    }

    pub fn with_body<V>(value: V) -> MessageBuilder
        where V: Into<Value> {
        MessageBuilder::new().with_body(value.into())
    }

    pub fn properties(&self) -> &Map {
        &self.properties
    }

    pub fn body(&self) -> &Value {
        &self.body
    }
}

pub struct MessageBuilder {
    map: LinkedHashMap<String, Value>,
    body: Value,
}

impl MessageBuilder {
    pub fn new() -> MessageBuilder {
        MessageBuilder { map: LinkedHashMap::new(), body: Value::Null }
    }

    pub fn with_property<K, V>(mut self, key: K, value: V) -> MessageBuilder
        where K: Into<String>, V: Into<Value> {
        self.map.insert(key.into(), value.into());
        self
    }

    pub fn with_body<V>(mut self, value: V) -> MessageBuilder
        where V: Into<Value> {
        self.body = value.into();
        self
    }

    pub fn build(self) -> Message {
        Message { properties: Map { map: self.map }, body: self.body }
    }
}

#[derive(PartialEq, Clone)]
pub struct Map {
    map: LinkedHashMap<String, Value>,
}

impl Map {
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.map.get(key)
    }
}

impl fmt::Debug for Map {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.map.fmt(f)
    }
}

impl Map {
    pub fn new() -> MapBuilder {
        MapBuilder { map: LinkedHashMap::new() }
    }
}

pub struct MapBuilder {
    map: LinkedHashMap<String, Value>,
}

impl MapBuilder {
    pub fn insert<K, V>(mut self, key: K, value: V) -> MapBuilder
        where K: Into<String>, V: Into<Value> {
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
    pub fn append<V>(mut self, value: V) -> ListBuilder where V: Into<Value> {
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
    Float64(f64),
    Boolean(bool),
    Bytes(Vec<u8>),
    List(List),
    Map(Map),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let message = Message::new()
            .with_body("Hello")
            .with_property("vehicles", List::new()
                .append("Aprilia")
                .append("Infiniti")
                .build()
            )
            .with_property("address", Map::new()
                .insert("street", "400 Beale ST")
                .insert("city", "San Francisco")
                .insert("state", "CA")
                .insert("zip", "94105")
                .build()
            )
            .build();
        println!("message = {:?}", message);
        let message = Message::new()
            .with_body("Wicked!!")
            .with_property("Hello", "World!")
            .with_property("age", 42)
            .with_property("weight", 175.5)
            .with_property("address",  "400 Beale ST APT 1403")
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

        assert_eq!(m.body(), &Value::Null);
    }

    #[test]
    fn map_as_body() {
        let m = Message::with_body(Map::new()
            .insert("fname", "Jimmie")
            .insert("lname", "Fulton")
            .build()
        ).build();

        println!("message = {:?}", &m);

        match m.body() {
            &Value::Map(ref map) => {
                assert_eq!(map.get("fname"), Some(&Value::from("Jimmie")));
                assert_eq!(map.get("lname"), Some(&Value::from("Fulton")));
            },
            _ => panic!("Map expected!")
        }
    }

    #[test]
    fn list_index() {
        let l = List::new().append("one").append("two").append("three").build();
        assert_eq!(l[0], Value::from("one"));
    }
}

