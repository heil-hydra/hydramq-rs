use linked_hash_map::LinkedHashMap;
use message::message::{Message, Key, Value};

pub struct Pipeline {
    handlers: LinkedHashMap<String, Box<Handler>>,
}

#[derive(Debug)]
pub enum PipelineDirection {
    Downstream,
    Upstream,
}

impl Default for PipelineDirection {
    fn default() -> Self {
        PipelineDirection::Downstream
    }
}

#[derive(Debug)]
pub enum PipelineFlow {
    Downstream,
    Upstream,
    DownstreamAndUpstream,
    Break,
}

impl Default for PipelineFlow {
    fn default() -> Self {
        PipelineFlow::DownstreamAndUpstream
    }
}

impl PipelineFlow {
    pub fn continue_downstream(&self) -> bool {
        match self {
            PipelineFlow::Downstream | PipelineFlow::DownstreamAndUpstream => true,
            _ => false
        }
    }

    pub fn continue_upstream(&self) -> bool {
        match self {
            PipelineFlow::Upstream | PipelineFlow::DownstreamAndUpstream => true,
            _ => false
        }
    }
}

impl Pipeline {
    fn process(&self, message: Message) {
        let mut context = PipelineContext::new(&self, message);

        if context.flow().continue_downstream() {
            for (key, handler) in self.handlers.iter() {
                context.set_handler_key(key);
                handler.handle_downstream(&mut context);
                if !context.flow().continue_downstream() {
                    break;
                }
            }
        }

        context.direction = PipelineDirection::Upstream;

        if context.flow().continue_upstream() {
            for (key, handler) in self.handlers.iter().rev() {
                context.set_handler_key(key);
                handler.handle_upstream(&mut context);
                if !context.flow().continue_upstream() {
                    break;
                }
            }
        }
    }
}

trait Handler {
    fn handle_downstream(&self, context: &mut PipelineContext) {

    }

    fn handle_upstream(&self, context: &mut PipelineContext) {

    }
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
    current_handler: Option<&'c str>,
    pipeline: &'c Pipeline,
    flow: PipelineFlow,
    direction: PipelineDirection,
}

impl<'c, 'm> PipelineContext<'c, 'm> {

    pub fn message(&self) -> &Message<'m> {
        &self.message
    }

    pub fn message_mut(&mut self) -> &mut Message<'m> {
        &mut self.message
    }

    pub fn set_message(&mut self, message: Message<'m>) {
        self.message = message;
    }

    pub fn current_handler_key(&self) -> &'c str {
        if let Some(handler_key) = self.current_handler {
            handler_key
        } else {
            panic!("Calls to handler_key are only allowed within a running Pipeline");
        }
    }

    fn set_handler_key(&mut self, handler_key: &'c str) {
        self.current_handler = Some(handler_key)
    }

    pub fn flow(&self) -> &PipelineFlow {
        &self.flow
    }

    pub fn set_flow(&mut self, flow: PipelineFlow) {
        self.flow = flow;
    }

    pub fn direction(&self) -> &PipelineDirection {
        &self.direction
    }

    pub fn pipeline(&self) -> &Pipeline {
        self.pipeline
    }

    pub fn new(pipeline: &'c Pipeline, message: Message<'m>) -> PipelineContext<'c, 'm> {
        PipelineContext {
            message,
            current_handler: None,
            pipeline,
            flow: Default::default(),
            direction: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct HeartbeatHandler;

    impl Handler for HeartbeatHandler {
        fn handle_downstream(&self, context: &mut PipelineContext) {
            print_details(context);
            if context.message().headers().contains_key(&Key::from("first")) {
                context.pipeline().process(Message::new());
            }
        }

        fn handle_upstream(&self, context: &mut PipelineContext) {
            print_details(context)
        }
    }

    struct DebugHandler;

    impl Handler for DebugHandler {
        fn handle_downstream(&self, context: &mut PipelineContext) {
            println!("Handler: {:?} ({:?}), Message: {:?}",
                     context.current_handler_key(), context.direction(), context.message());
        }

        fn handle_upstream(&self, context: &mut PipelineContext) {
            print_details(context);
        }
    }

    fn print_details(context: &PipelineContext) {
        println!("Handler: {} ({:?})", context.current_handler_key(), context.direction());
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