use bit_set::BitSet;
use crate::command::event::{FormatDescriptionLogEvent, LogContext, LogEvent, LogHeader, UnknownLogEvent};
use crate::command::event::LogEvent::UnknownLog;
use crate::instance::log_buffer::LogBuffer;

struct LogDecoder {
    handle_set: BitSet,
}

impl LogDecoder {
    pub fn new() -> Self {
        let mut decoder = Self { handle_set: BitSet::new() };
        decoder.handle_set.insert(165);
        decoder
    }


    pub fn from(from_index: usize, to_index: usize) -> Self{
        let mut decoder = LogDecoder {
            handle_set: Default::default()
        };
        decoder.handle_set.insert(165);
        for i in from_index..to_index {
            decoder.handle_set.insert(i);
        }
        decoder
    }

    pub fn handler_from_to(&mut self, from_index: usize, to_index: usize) {
        for i in from_index..to_index {
            self.handle_set.insert(i);
        }
    }

    pub fn handler(&mut self, flag_index: usize)  {
        self.handle_set.insert(flag_index);
    }


    pub fn decode(&self, buffer: &mut LogBuffer, context: &LogContext) -> LogEvent {
        let limit = buffer.limit();

        if limit >= FormatDescriptionLogEvent::LOG_EVENT_HEADER_LEN {
            let header = LogHeader::from_buffer(buffer, context.description_event()).unwrap();

            let len = header.event_len();

            if limit >= len {
                let mut event;

                if self.handle_set.contains(header.kind()) {
                    buffer.new_limit(len);

                    event = Self::decode_event(buffer, &header, context)
                } else {
                    event = UnknownLog(UnknownLogEvent::from(&header).unwrap());
                }
                let header = event.header_mut().unwrap();
                header.set_log_file_name(context.position().file_name().clone());
                let ent = event.event_mut().unwrap();
                ent.set_semival(buffer.seminal());

                buffer.consume(len);
                return event
            }

        }
        buffer.rewind();
        LogEvent::Null(Option::None)
    }

    pub fn decode_event(buffer: &mut LogBuffer,header: &LogHeader, context: &LogContext) -> LogEvent{
        LogEvent::Null(Option::None)
    }
}