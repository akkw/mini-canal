pub trait SinkFunction<E> {
    fn sink(&self, event: E) -> bool;
}
struct DefaultSink;

impl<E> SinkFunction<E> for DefaultSink{
    fn sink(&self, event: E) -> bool {
        todo!()
    }
}


pub trait MultiStageCoprocessor {}