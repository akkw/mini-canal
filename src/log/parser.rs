use std::string;
use charsets::Charset;
use str_utils::{StartsWithIgnoreAsciiCase, StartsWithIgnoreCase};
use crate::command::event::{Event, LogEvent, QueryLogEvent};
use crate::command::event::LogEvent::UnknownLog;
use crate::protocol::mini_canal_entry::{Entry, TransactionBegin};
use crate::protocol::mini_canal_entry::Header_oneof_eventType_present::eventType;

pub struct LogEventConvert {
    charset: Charset,
}

impl LogEventConvert {
    pub const XA_XID: &'static str = "XA_XID";
    pub const XA_TYPE: &'static str = "XA_TYPE";
    pub const XA_START: &'static str = "XA START";
    pub const XA_END: &'static str = "XA END";
    pub const XA_COMMIT: &'static str = "XA COMMIT";
    pub const XA_ROLLBACK: &'static str = "XA ROLLBACK";
    pub const ISO_8859_1: &'static str = "ISO-8859-1";
    pub const UTF_8: &'static str = "UTF-8";
    pub const TINYINT_MAX_VALUE: usize = 256;
    pub const SMALLINT_MAX_VALUE: usize = 65536;
    pub const MEDIUMINT_MAX_VALUE: usize = 16777216;
    pub const INTEGER_MAX_VALUE: usize = 4294967296;
    pub const BIGINT_MAX_VALUE: u128 = 18446744073709551616;
    pub const VERSION: usize = 1;
    pub const BEGIN: &'static str  = "BEGIN";
    pub const COMMIT: &'static str  = "COMMIT";


    pub fn new() -> Self {
        Self { charset: Charset::Utf8 }
    }


    pub fn parse(log_event: &mut LogEvent, is_seek: bool) -> Option<Entry> {
        if let UnknownLog(u) = log_event {
            return Option::None;
        }

        let event_type = log_event.header_mut().unwrap().kind();

        match event_type {
            Event::QUERY_EVENT => {
                return Self::parse_query_event(log_event.query_log_event().unwrap());
            }
            _ => {}
        }

        Option::Some(Entry::new())
    }

    fn parse_query_event(event: &QueryLogEvent) -> Option<Entry> {
        let query = event.query().as_ref().unwrap();

        if query.starts_with_ignore_case(query) {
            let begin = TransactionBegin::new();
        }

        Option::Some(Entry::new())
    }
}