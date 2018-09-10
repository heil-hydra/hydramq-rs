use linked_hash_map::LinkedHashMap;
use std::borrow::Cow;
use uuid::Uuid;
use chrono::{DateTime, UTC};

#[derive(Debug, Clone, PartialEq)]
pub struct Message<'a> {
    timestamp: Option<Timestamp>,
    expiration: Option<Timestamp>,
    correlation_id: Option<Uuid>,
    headers: Map<'a>,
    body: Option<Value<'a>>,
}

impl<'a> Message<'a> {
    pub fn new() -> Message<'a> {
        Message {
            timestamp: None,
            expiration: None,
            correlation_id: None,
            headers: Map::new(),
            body: None
        }
    }

    pub fn timestamp(&self) -> Option<Timestamp> {
        self.timestamp
    }

    pub fn set_timestamp(&mut self, value: Option<Timestamp>) {
        self.timestamp = value;
    }

    pub fn expiration(&self) -> Option<Timestamp> {
        self.expiration
    }

    pub fn set_expiration(&mut self, value: Option<Timestamp>) {
        self.expiration = value;
    }

    pub fn correlation_id(&self) -> Option<Uuid> {
        self.correlation_id
    }

    pub fn set_correlation_id(&mut self, value: Option<Uuid>) {
        self.correlation_id = value;
    }

    pub fn headers(&self) -> &Map<'a> {
        &self.headers
    }

    pub fn headers_mut(&mut self) -> &mut Map<'a> {
        &mut self.headers
    }

    pub fn body(&self) -> Option<&Value<'a>> {
        match self.body {
            Some(ref value) => Some(value),
            None => None,
        }
    }

    pub fn set_body<V: Into<Value<'a>>>(&mut self, value: Option<V>) {
        self.body = value.map(|v| v.into()).or(None);
    }
}

pub struct MessageBuilder<'a> {
    message: Message<'a>,
}

impl<'a> MessageBuilder<'a> {
    pub fn new() -> MessageBuilder<'a> {
        MessageBuilder {
            message: Message::new()
        }
    }

    pub fn with_timestamp(mut self, timestamp: Timestamp) -> MessageBuilder<'a> {
        self.message.set_timestamp(Some(timestamp));
        self
    }

    pub fn with_expiration(mut self, expiration: Timestamp) -> MessageBuilder<'a> {
        self.message.set_expiration(Some(expiration));
        self
    }

    pub fn with_correlation_id(mut self, correlation_id: Uuid) -> MessageBuilder<'a> {
        self.message.set_correlation_id(Some(correlation_id));
        self
    }

    pub fn with_header<K, V>(mut self, key: K, value: V) -> MessageBuilder<'a>
    where
        K: Into<Key<'a>>,
        V: Into<Value<'a>>,
    {
        self.message.headers_mut().insert(key.into(), value.into());
        self
    }

    pub fn with_body<V>(mut self, body: V) -> MessageBuilder<'a>
    where
        V: Into<Value<'a>>,
    {
        self.message.set_body(Some(body.into()));
        self
    }


    pub fn build(self) -> Message<'a> {
        self.message
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Key<'a> {
    Str(Cow<'a, str>),
    I32(i32),
}

impl<'a> From<&'a str> for Key<'a> {
    fn from(key: &'a str) -> Self {
        Key::Str(Cow::Borrowed(key))
    }
}

impl<'a> From<String> for Key<'a> {
    fn from(key: String) -> Self {
        Key::Str(Cow::Owned(key))
    }
}

impl<'a> From<i32> for Key<'a> {
    fn from(key: i32) -> Self {
        Key::I32(key)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct List<'a> {
    inner: Vec<Value<'a>>,
}

impl<'a> List<'a> {
    pub fn new() -> List<'a> {
        List { inner: Vec::new() }
    }

    pub fn builder() -> ListBuilder<'a> { ListBuilder::new() }

    pub fn iter(&self) -> std::slice::Iter<Value<'a>> {
        self.inner.iter()
    }

    pub fn push<V: Into<Value<'a>>>(&mut self, value: V) {
        self.inner.push(value.into());
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

pub struct ListBuilder<'a> {
    list: List<'a>,
}

impl<'a> ListBuilder<'a> {
    pub fn new() -> ListBuilder<'a> {
        ListBuilder{ list: List::new() }
    }

    pub fn push<V>(mut self, value: V) -> ListBuilder<'a>
        where
            V: Into<Value<'a>>,
    {
        self.list.push(value.into());
        self
    }

    pub fn push_mut<V>(&mut self, value: V)
        where
            V: Into<Value<'a>>,
    {
        self.list.push(value.into());
    }

    pub fn build(self) -> List<'a> {
        self.list
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Map<'a> {
    inner: LinkedHashMap<Key<'a>, Value<'a>>,
}

impl<'a> Map<'a> {
    pub fn new() -> Map<'a> {
        Map{ inner: LinkedHashMap::new() }
    }

    pub fn insert<K: Into<Key<'a>>, V: Into<Value<'a>>>(&mut self, key: K, value: V) {
        self.inner.insert(key.into(), value.into());
    }

    pub fn get(&self, key: &Key<'a>) -> Option<&Value<'a>> {
        self.inner.get(key)
    }

    pub fn iter(&self) -> linked_hash_map::Iter<Key<'a>, Value<'a>> {
        self.inner.iter()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn contains_key(&self, key: &Key<'a>) -> bool {
        self.inner.contains_key(key)
    }
}

pub struct MapBuilder<'a> {
    map: Map<'a>,
}

impl<'a> MapBuilder<'a> {
    pub fn new() -> MapBuilder<'a> {
        MapBuilder{ map: Map::new() }
    }

    pub fn insert<K, V>(mut self, key: K, value: V) -> MapBuilder<'a>
        where
            K: Into<Key<'a>>,
            V: Into<Value<'a>>,
    {
        self.map.insert(key.into(), value.into());
        self
    }

    pub fn insert_mut<K, V>(&mut self, key: K, value: V)
        where
            K: Into<Key<'a>>,
            V: Into<Value<'a>>,
    {
        self.map.insert(key.into(), value.into());
    }

    pub fn build(self) -> Map<'a> {
        self.map
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value<'a> {
    Null,
    Str(Cow<'a, str>),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Bool(bool),
    Bytes(Cow<'a, [u8]>),
    List(List<'a>),
    Map(Map<'a>),
    Uuid(Uuid),
    Timestamp(Timestamp),
//    Key(Key<'a>),
}

impl<'a> From<&'a str> for Value<'a> {
    fn from(value: &'a str) -> Self {
        Value::Str(Cow::Borrowed(value))
    }
}

impl<'a> From<String> for Value<'a> {
    fn from(value: String) -> Self {
        Value::Str(Cow::Owned(value))
    }
}

impl<'a> From<i32> for Value<'a> {
    fn from(value: i32) -> Self {
        Value::I32(value)
    }
}

impl<'a> From<i64> for Value<'a> {
    fn from(value: i64) -> Self {
        Value::I64(value)
    }
}

impl<'a> From<f32> for Value<'a> {
    fn from(value: f32) -> Self {
        Value::F32(value)
    }
}

impl<'a> From<f64> for Value<'a> {
    fn from(value: f64) -> Self {
        Value::F64(value)
    }
}

impl<'a> From<bool> for Value<'a> {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}

impl<'a> From<Map<'a>> for Value<'a> {
    fn from(value: Map<'a>) -> Self { Value::Map(value) }
}

impl<'a> From<List<'a>> for Value<'a> {
    fn from(value: List<'a>) -> Self { Value::List(value) }
}

impl<'a> From<Timestamp> for Value<'a> {
    fn from(value: Timestamp) -> Self { Value::Timestamp(value) }
}

pub type Timestamp = DateTime<UTC>;


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construct_simple_mesage() {
        let mut message = Message::new();

        message.set_body(Some("value"));
        assert_eq!(message.body(), Some(&Value::from("value")));

        message.headers_mut().insert("Null", Value::Null);
        message.headers_mut().insert("Str", "string");
        message.headers_mut().insert("I32", 32);
        message.headers_mut().insert("I64", 64i64);
        message.headers_mut().insert("F32", 32.32f32);
        message.headers_mut().insert("F64", 64.64f64);
        message.headers_mut().insert("Bool(true)", true);
        message.headers_mut().insert("Bool(false)", false);

        assert_eq!(message.headers().len(), 8);
        assert_eq!(message.headers().get(&Key::from("Null")), Some(&Value::Null));
        assert_eq!(message.headers().get(&Key::from("Str")), Some(&Value::from("string")));
        assert_eq!(message.headers().get(&Key::from("I32")), Some(&Value::from(32i32)));
        assert_eq!(message.headers().get(&Key::from("I64")), Some(&Value::from(64i64)));
        assert_eq!(message.headers().get(&Key::from("F32")), Some(&Value::from(32.32f32)));
        assert_eq!(message.headers().get(&Key::from("F64")), Some(&Value::from(64.64f64)));
        assert_eq!(message.headers().get(&Key::from("Bool(true)")), Some(&Value::from(true)));
        assert_eq!(message.headers().get(&Key::from("Bool(false)")), Some(&Value::from(false)));
        assert_eq!(message.headers().get(&Key::from("Missing")), None);

        for (key, value) in message.headers().iter() {
            println!("{:?}: {:?}", key, value);
        }
    }
}
