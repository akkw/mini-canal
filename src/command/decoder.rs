use bit_set::BitSet;
use chrono::format::parse;
use crate::command::event::{*};
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


    pub fn from(from_index: usize, to_index: usize) -> Self {
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

    pub fn handler(&mut self, flag_index: usize) {
        self.handle_set.insert(flag_index);
    }


    pub fn decode(&self, buffer: &mut LogBuffer, context: &mut LogContext) -> LogEvent {
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
                return event;
            }
        }
        buffer.rewind();
        LogEvent::Null(Option::None)
    }

    pub fn decode_event(buffer: &mut LogBuffer, header: &LogHeader, context: &mut LogContext) -> LogEvent {
        let checksum_alg;
        if header.kind() != FORMAT_DESCRIPTION_EVENT {
            checksum_alg = context.description_event().start_log_event_v3().event().header().checksum_alg();
        } else {
            checksum_alg = header.checksum_alg();
        }
        if checksum_alg != BINLOG_CHECKSUM_ALG_OFF && checksum_alg != BINLOG_CHECKSUM_ALG_UNDEF {
            buffer.new_limit(header.event_len() - BINLOG_CHECKSUM_LEN as usize).expect("TODO: panic message");
        }
        match header.kind() {
            QUERY_EVENT => {
                let event = QueryLogEvent::from(header, buffer, context.description_event()).unwrap();
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::QueryLog(event);
            }
            XID_EVENT => {
                let event = XidLogEvent::from(header, buffer, context.description_event()).unwrap();
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::XidLog(event);
            }
            TABLE_MAP_EVENT => {
                let event = TableMapLogEvent::from(header, buffer, context.description_event()).unwrap();
                context.position().set_position(header.log_pos() as usize);
                context.put_map_table(event.clone());
                return LogEvent::TableMapLog(event);
            }
            WRITE_ROWS_EVENT_V1 |
            WRITE_ROWS_EVENT => {
                let mut event = WriteRowsLogEvent::from(header, buffer, context.description_event()).unwrap();
                context.position().set_position(header.log_pos() as usize);
                event.event_mut().fill_table(context);
                return LogEvent::WriteRowsLog(event);
            }
            UPDATE_ROWS_EVENT_V1 |
            UPDATE_ROWS_EVENT => {
                let mut event = UpdateRowsLogEvent::from(header, buffer, context.description_event()).unwrap();
                context.position().set_position(header.log_pos() as usize);
                event.rows_log_event_mut().fill_table(context);
                return LogEvent::UpdateRowsLog(event);
            }
            DELETE_ROWS_EVENT_V1 |
            DELETE_ROWS_EVENT => {
                let mut event = DeleteRowsLogEvent::from(header, buffer, context.description_event()).unwrap();
                context.position().set_position(header.log_pos() as usize);
                event.rows_log_event_mut().fill_table(context);
            }
            ROTATE_EVENT => {
                let event = RotateLogEvent::from(header, buffer, context.description_event()).unwrap();
                let file_name = event.file_name().as_ref().unwrap();
                let position = LogPosition::from_name_position(String::from(file_name), event.position() as usize);
                context.set_position(position);
                return LogEvent::RotateLog(event);
            }
            LOAD_EVENT |
            NEW_LOAD_EVENT => {
                let event = LoadLogEvent::from(header, buffer, context.description_event()).unwrap();
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::LoadLog(event);
            }
            SLAVE_EVENT => {
                println!("Skipping unsupported SLAVE_EVENT from: {}", context.getLogPosition())
            }
            CREATE_FILE_EVENT => {
                let event = CreateFileLogEvent::from(header, buffer, context.description_event());
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::CreateFileLog(event);
            }


            _ => todo!()
        }

        LogEvent::Null(Option::None)
    }
}