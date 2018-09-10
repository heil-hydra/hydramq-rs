use linked_hash_map::LinkedHashMap;
use message::message::{Message, Key, Value};

pub struct Pipeline {
    handlers: LinkedHashMap<String, Box<Handler>>,
}

impl Pipeline {
    fn process(&self, message: Message) {
        let mut context = PipelineContext::new(&self, message);

        for (key, handler) in self.handlers.iter() {
            context.set_handler_key(key);
            handler.handle_downstream(&mut context);
        }

        for (key, handler) in self.handlers.iter().rev() {
            context.set_handler_key(key);
            handler.handle_upstream(&mut context);
        }
    }
}

trait Handler {
    fn handle_downstream(&self, context: &mut PipelineContext);
    fn handle_upstream(&self, context: &mut PipelineContext);
}

struct PipelineBuilder {
    handlers: LinkedHashMap<String, Box<Handler>>,
}

impl PipelineBuilder{
    fn append_handler(&mut self, name: String, handler: Box<Handler>) {
        self.handlers.insert(name, handler);
    }

    fn build(self) -> Pipeline {
        Pipeline { handlers: self.handlers }
    }
}

impl Default for PipelineBuilder {
    fn default() -> Self {
        PipelineBuilder { handlers: Default::default() }
    }
}

pub struct PipelineContext<'c, 'm> {
    message: Message<'m>,
    handler_key: Option<&'c str>,
    pipeline: &'c Pipeline,
}

impl<'c, 'm> PipelineContext<'c, 'm> {

    fn message(&mut self) -> &mut Message<'m> {
        &mut self.message
    }

    fn set_message(&mut self, message: Message<'m>) {
        self.message = message;
    }

    fn handler_key(&self) -> &'c str {
        if let Some(handler_key) = self.handler_key {
            handler_key
        } else {
            panic!("Calls to handler_key are only allowed within a running Pipeline");
        }
    }

    fn set_handler_key(&mut self, handler_key: &'c str) {
        self.handler_key = Some(handler_key)
    }

    fn pipeline(&self) -> &Pipeline {
        self.pipeline
    }

    fn new(pipeline: &'c Pipeline, message: Message<'m>) -> PipelineContext<'c, 'm> {
        PipelineContext {
            message,
            handler_key: None,
            pipeline,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct HeartbeatHandler;

    impl Handler for HeartbeatHandler {
        fn handle_downstream(&self, context: &mut PipelineContext) {
            println!("Handler: {:?} (downstream)", context.handler_key());
            if context.message().headers().contains_key(&Key::from("first")) {
                context.pipeline().process(Message::new());
            }
        }

        fn handle_upstream(&self, context: &mut PipelineContext) {
            println!("Handler: {:?} (upstream)", context.handler_key());
        }
    }

    struct DebugHandler;

    impl Handler for DebugHandler {
        fn handle_downstream(&self, context: &mut PipelineContext) {
            println!("Handler: {:?} (downstream), Message: {:?}", context.handler_key(), context.message());
        }

        fn handle_upstream(&self, context: &mut PipelineContext) {
        }
    }

    #[test]
    fn test_api() {
        let mut builder = PipelineBuilder::default();
        builder.append_handler("Heartbeat".to_owned(), Box::new(HeartbeatHandler));
        builder.append_handler("Debug".to_owned(), Box::new(DebugHandler));

        let pipeline = builder.build();

        for i in 0 .. 1000 {
            let mut message = Message::new();
            message.set_body(Some(Value::from(i)));
            message.headers_mut().insert("first", true);
            pipeline.process(message);
        }
    }
}