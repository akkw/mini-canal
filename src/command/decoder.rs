use bit_set::BitSet;
use chrono::format::parse;
use crate::command::event::{*};
use crate::command::event::LogEvent::UnknownLog;
use crate::instance::log_buffer::LogBuffer;

pub struct LogDecoder {
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

                    event = Self::decode_event(buffer, &header, context);
                    buffer.new_limit(limit);
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
                println!("Skipping unsupported SLAVE_EVENT from: {}", context.position().position())
            }
            CREATE_FILE_EVENT => {
                let event = CreateFileLogEvent::from(header, buffer, context.description_event());
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::CreateFileLog(event);
            }
            APPEND_BLOCK_EVENT => {
                let event = AppendBlockLogEvent::from(header, buffer, context.description_event());
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::AppendBlockLog(event);
            }
            DELETE_FILE_EVENT => {
                let event = DeleteFileLogEvent::from(header, buffer, context.description_event());
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::DeleteFileLog(event);
            }
            EXEC_LOAD_EVENT => {
                let event = ExecuteLoadLogEvent::from(header, buffer, context.description_event());
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::ExecuteLoadLog(event);
            }
            START_EVENT_V3 => {
                let event = StartLogEventV3::from(header, buffer, context.description_event()).unwrap();
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::StartLogV3(event);
            }
            STOP_EVENT => {
                let event = StopLogEvent::from(header, buffer, context.description_event()).unwrap();
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::StopLog(event);
            }
            INTVAR_EVENT => {
                let event = InvarianceLogEvent::from(header, buffer, context.description_event()).unwrap();
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::InvarianceLog(event);
            }
            RAND_EVENT => {
                let event = RandLogEvent::from(header, buffer, context.description_event()).unwrap();
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::RandLog(event);
            }
            USER_VAR_EVENT => {
                let event = UserVarLogEvent::from(header, buffer, context.description_event()).unwrap();
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::UserVarLog(event);
            }
            FORMAT_DESCRIPTION_EVENT => {
                let description_event = FormatDescriptionLogEvent::from(header, buffer, context.description_event()).unwrap();
                context.set_description_event(description_event.clone());
                return LogEvent::FormatDescriptionLog(description_event)
            }
            PRE_GA_WRITE_ROWS_EVENT |
            PRE_GA_UPDATE_ROWS_EVENT |
            PRE_GA_DELETE_ROWS_EVENT => {
                println!("Skipping unsupported SLAVE_EVENT from: {}", context.position().position())
            }
            BEGIN_LOAD_QUERY_EVENT => {
                let event = BeginLoadQueryLogEvent::from(header, buffer, context.description_event());
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::BeginLoadQueryLog(event);
            }
            EXECUTE_LOAD_QUERY_EVENT => {
                let event = ExecuteLoadQueryLogEvent::from(header, buffer, context.description_event()).unwrap();
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::ExecuteLoadQueryLog(event);
            }
            INCIDENT_EVENT => {
                let event = IncidentLogEvent::from(header, buffer, context.description_event()).unwrap();
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::IncidentLog(event);
            }
            HEARTBEAT_LOG_EVENT => {
                let event = HeartbeatLogEvent::from(header, buffer, context.description_event()).unwrap();
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::HeartbeatLog(event);
            }
            IGNORABLE_LOG_EVENT => {
                let event = IgnorableLogEvent::from(header, buffer, context.description_event());
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::IgnorableLog(event);
            }
            ROWS_QUERY_LOG_EVENT => {
                let event = RowsQueryLogEvent::from(header, buffer, context.description_event()).unwrap();
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::RowsQueryLog(event);
            }
            PARTIAL_UPDATE_ROWS_EVENT => {
                let event = UpdateRowsLogEvent::from(header, buffer, context.description_event()).unwrap();
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::UpdateRowsLog(event);
            }
            GTID_LOG_EVENT |
            ANONYMOUS_GTID_LOG_EVENT => {
                todo!()
            }
            PREVIOUS_GTIDS_LOG_EVENT => {
                let event = PreviousGtidsLogEvent::from(header, buffer, context.description_event());
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::PreviousGtidsLog(event);
            }
            TRANSACTION_CONTEXT_EVENT => {
                let event = TransactionContextLogEvent::from(header, buffer, context.description_event()).unwrap();
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::TransactionContextLog(event);
            }
            TRANSACTION_PAYLOAD_EVENT=> {
                let event = TransactionPayloadLogEvent::from(header, buffer, context.description_event()).unwrap();
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::TransactionPayloadLog(event);
            }
            VIEW_CHANGE_EVENT => {
                let event = ViewChangeEvent::from(header, buffer, context.description_event()).unwrap();
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::ViewChange(event);
            }
            XA_PREPARE_LOG_EVENT => {
                let event = XaPrepareLogEvent::from(header, buffer, context.description_event()).unwrap();
                context.position().set_position(header.log_pos() as usize);
                return LogEvent::XaPrepareLog(event);
            }
            ANNOTATE_ROWS_EVENT => {
                todo!()
            }
            BINLOG_CHECKPOINT_EVENT => {
                todo!()
            }
            GTID_EVENT => {
                todo!()
            }
            GTID_LIST_EVENT => {
                todo!()
            }
            START_ENCRYPTION_EVENT => {
                todo!()
            }
            _ =>  {
                /*
                 * Create an object of Ignorable_log_event for unrecognized
                 * sub-class. So that SLAVE SQL THREAD will only update the
                 * position and continue.
                 */
                if buffer.get_uint16().unwrap() & LOG_EVENT_IGNORABLE_F as u16 > 0 {
                    let event = IgnorableLogEvent::from(header, buffer, context.description_event());
                    context.position().set_position(header.log_pos() as usize);
                    return LogEvent::IgnorableLog(event);
                } else {
                    println!("Skipping unsupported SLAVE_EVENT from: {}", context.position().position())
                }
            }
        }
        context.position().set_position(header.log_pos() as usize);

        LogEvent::UnknownLog(UnknownLogEvent::from(header).unwrap())
    }
}