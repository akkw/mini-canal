use std::string;
use charsets::Charset;
use protobuf::{Message, RepeatedField};
use str_utils::{StartsWithIgnoreAsciiCase, StartsWithIgnoreCase};
use substring::Substring;
use crate::command::event::{Event, LogEvent, LogHeader, QueryLogEvent};
use crate::command::event::LogEvent::UnknownLog;
use crate::protocol::mini_canal_entry::{Entry, EntryType, EventType, Header, Pair, TransactionBegin, Type};
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
    pub const VERSION: i32 = 1;
    pub const BEGIN: &'static str = "BEGIN";
    pub const COMMIT: &'static str = "COMMIT";


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
            let mut begin = TransactionBegin::new();
            begin.set_threadId(event.session_id() as i64);
            let mut list = RepeatedField::new();
            list.push(Self::create_special_pair(Self::XA_TYPE.into(), Self::XA_START.into()));
            list.push(Self::create_special_pair(Self::XA_XID.into(), Self::get_xa_xid(query.into(), Self::XA_START.into())));
            begin.set_props(list);
            let header = Self::create_header(event.event().header(), Option::Some("".into()), Option::Some("".into()), None);
            return Option::Some(Self::create_entry(header, EntryType::TRANSACTIONBEGIN, begin.write_to_bytes().unwrap()));
        }

        Option::Some(Entry::new())
    }

    fn create_entry(header: Header, entry_type: EntryType, bytes: Vec<u8>) -> Entry {
        let mut entry = Entry::new();
        entry.set_header(header);
        entry.set_entryType(entry_type);
        entry.set_storeValue(bytes);
        entry
    }

    fn create_special_pair(key: String, value: String) -> Pair {
        let mut pair = Pair::new();
        pair.set_key(key);
        pair.set_value(value);
        pair
    }

    fn get_xa_xid(query: String, kind: String) -> String {
        query.substring(query.find(kind.as_str()).unwrap() + kind.len(), query.len()).into()
    }

    fn create_header(header: &LogHeader, schema_name: Option<String>, table_name: Option<String>, event_type: Option<EventType>) -> Header {
        Self::create_header_rows(header, schema_name, table_name, event_type, -1)
    }

    fn create_header_rows(log_header: &LogHeader, schema_name: Option<String>, table_name: Option<String>, event_type: Option<EventType>, rows_count: i32) -> Header {
        let mut header = Header::new();
        header.set_version(Self::VERSION);
        // 记录的是该binlog的start offset
        header.set_logfileOffset(log_header.log_pos() as i64 - log_header.event_len() as i64);
        header.set_logfileName(log_header.log_file_name().as_ref().unwrap().into());
        header.set_serverId(log_header.server_id() as i64);
        header.set_serverenCode(Self::UTF_8.into());
        header.set_executeTime(log_header.when() as i64 * 1000);
        header.set_sourceType(Type::MYSQL);
        if Option::None != event_type {
            header.set_eventType(event_type.unwrap());
        }
        if Option::None != schema_name {
            header.set_schemaName(schema_name.unwrap());
        }
        if Option::None != table_name {
            header.set_tableName(table_name.unwrap());
        }
        header.set_eventLength(log_header.event_len() as i64);

        if let Option::Some(s) = log_header.gtid_set_str() {
            header.set_gtid(s)
        }
        let mut list = RepeatedField::new();

        if let Option::Some(s) = log_header.get_current_gtid() {
            let pair = Self::create_special_pair("curtGtid".into(), s);
            list.push(pair);
        }

        if let Option::Some(s) = log_header.get_current_gtid_sn() {
            let pair = Self::create_special_pair("curtGtidSn".into(), s);
            list.push(pair);
        }

        if let Option::Some(s) = log_header.get_current_gtid_last_commit() {
            let pair = Self::create_special_pair("curtGtidLct".into(), s);
            list.push(pair);
        }
        if rows_count > 0 {
            let pair = Self::create_special_pair("rowsCount".into(), s);
            list.push(pair);
        }

        if list.len() != 0 {
            header.set_props(list);
        }
        header
    }
}