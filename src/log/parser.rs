use std::f32::consts::E;
use std::string;
use bit_set::BitSet;
use charsets::Charset;
use encoding::all::ISO_8859_1;
use encoding::{DecoderTrap, Encoding};
use protobuf::{Message, RepeatedField};
use str_utils::{StartsWithIgnoreAsciiCase, StartsWithIgnoreCase};
use substring::Substring;
use crate::channel::mysql_socket::MysqlConnection;
use crate::command::types::Types;
use crate::instance::table_meta_cache::TableMetaCache;
use crate::log::event::{*};
use crate::log::event::LogEvent::UnknownLog;
use crate::log::log_buffer::RowsLogBuffer;
use crate::log::metadata::TableMeta;
use crate::protocol::mini_canal_entry::{Column, Entry, EntryType, EventType, Header, Pair, RowChange, RowData, TransactionBegin, TransactionEnd, Type};
use crate::protocol::mini_canal_entry::Header_oneof_eventType_present::eventType;
use crate::StringResult;

pub struct LogEventConvert {
    charset: Charset,
    table_cache: Option<TableMetaCache>,
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
        Self { charset: Charset::Utf8, table_cache: None }
    }

    pub fn from(table_cache: TableMetaCache) -> Self {
        Self { charset: Charset::Utf8, table_cache: Option::Some(table_cache) }
    }
    pub fn parse(&mut self, log_event: &mut LogEvent, is_seek: bool) -> Option<Entry> {
        if let UnknownLog(u) = log_event {
            return Option::None;
        }

        let event_type = log_event.header_mut().unwrap().kind();

        match event_type {
            Event::QUERY_EVENT => {
                return Self::parse_query_event(log_event.query_log_event().unwrap());
            }
            Event::XID_EVENT => {
                return Self::parse_xid_event(log_event.xid_log_event().unwrap());
            }
            Event::TABLE_MAP_EVENT => {
                Self::parse_table_map_event(log_event.table_map_log_event_mut().unwrap());
            }
            Event::WRITE_ROWS_EVENT_V1 |
            Event::WRITE_ROWS_EVENT => {
                return self.parse_rows_event(log_event.write_rows_log_event().unwrap().row_log_event());
            }
            Event::UPDATE_ROWS_EVENT_V1 |
            Event::PARTIAL_UPDATE_ROWS_EVENT |
            Event::UPDATE_ROWS_EVENT => {
                return self.parse_rows_event(log_event.update_rows_log().unwrap().rows_log_event());
            }
            Event::DELETE_ROWS_EVENT_V1 |
            Event::DELETE_ROWS_EVENT => {
                return self.parse_rows_event(log_event.delete_rows_log_event().unwrap().rows_log_event());
            }
            Event::ROWS_QUERY_LOG_EVENT => {
                self.parse_rows_query_event(log_event.rows_query_log_event().unwrap());
            }

            _ => {}
        }

        Option::None
    }

    fn parse_rows_query_event(&self, event: &RowsQueryLogEvent) -> Option<Entry> {
        todo!()
    }
    fn parse_rows_event(&mut self, write_rows_log_event: &RowsLogEvent) -> Option<Entry> {
        self.parse_rows_event_table_meta(write_rows_log_event, None).unwrap()
    }

    fn parse_rows_event_table_meta(&mut self, event: &RowsLogEvent, mut table_meta: Option<&TableMeta>) -> StringResult<Option<Entry>> {
        let table = event.table_map_log_event();
        let full_name = format!("{}.{}", table.dbname().as_ref().unwrap().as_str(), table.tblname().as_ref().unwrap().as_str());
        let table_meta = self.table_cache.as_mut().unwrap().get_table_meta_by_db(full_name.as_str());

        let event_type;
        let kind = event.event().header().kind();

        if Event::WRITE_ROWS_EVENT_V1 == kind || Event::WRITE_ROWS_EVENT == kind {
            event_type = EventType::INSERT;
        } else if Event::UPDATE_ROWS_EVENT_V1 == kind || Event::UPDATE_ROWS_EVENT == kind ||
            Event::PARTIAL_UPDATE_ROWS_EVENT == kind {
            event_type = EventType::UPDATE;
        } else if Event::DELETE_ROWS_EVENT_V1 == kind || Event::DELETE_ROWS_EVENT == kind {
            event_type = EventType::DELETE;
        } else {
            return Result::Err(format!("unsupport event type: {}", event.event().header().kind()));
        }

        let mut change = RowChange::new();
        change.set_tableId(event.table_id() as i64);
        change.set_isDdl(false);
        change.set_eventType(event_type);
        let mut buffer = event.get_rows_buf(self.charset.to_string());
        let columns = event.columns();
        let change_columns = event.change_columns();
        let mut row_change_list = RepeatedField::new();
        let mut table_error = false;
        let mut rows_count = 0;
        while buffer.next_one_row_after(columns, false) {
            let mut row_data = RowData::new();
            if EventType::INSERT == event_type {
                table_error |= self.parse_one_row(&mut row_data, event, &mut buffer, columns, true, table_meta.as_ref());
            } else if EventType::DELETE == event_type {
                table_error |= self.parse_one_row(&mut row_data, event, &mut buffer, columns, false, table_meta.as_ref());
            } else {
                table_error |= self.parse_one_row(&mut row_data, event, &mut buffer, columns, false, table_meta.as_ref());
                if !buffer.next_one_row_after(change_columns, true) {
                    row_change_list.push(row_data);
                    break;
                }
                table_error |= self.parse_one_row(&mut row_data, event, &mut buffer, columns, true, table_meta.as_ref());
            }

            rows_count += 1;
            row_change_list.push(row_data);
        }
        change.set_rowDatas(row_change_list);

        let table = event.table_map_log_event();
        let header = Self::create_header_rows(event.event().header(),
                                              table.dbname().clone(),
                                              table.tblname().clone(),
                                              Option::Some(event_type),
                                              rows_count);


        if table_error {
            let entry = Self::create_entry(header, EntryType::ROWDATA, change.write_to_bytes().unwrap());
            println!("table parser error : {:?}storeValue: {:?}", entry, change);
            return Result::Ok((Option::None));
        } else {
            let entry = Self::create_entry(header, EntryType::ROWDATA, change.write_to_bytes().unwrap());
            return return Result::Ok((Option::Some(entry)));;
        }
    }

    fn parse_one_row(&self, row_data: &mut RowData, event: &RowsLogEvent, buffer: &mut RowsLogBuffer, cols: &BitSet, is_after: bool, table_meta: Option<&TableMeta>) -> bool {
        let column_cont = event.table_map_log_event().column_cnt() as usize;
        let column_info = event.table_map_log_event().column_info();
        let mut i = 0;
        let mut row_data_list = RepeatedField::new();
        while i < column_cont {
            let info = &column_info[i];
            if !cols.contains(i) {
                continue;
            }
            let mut field_meta;
            if let Option::Some(meta) = table_meta {
                field_meta = Option::Some(&meta.fields()[i]);
            } else {
                field_meta = Option::None
            }


            let mut column = Column::new();

            if let Option::Some(meta) = field_meta {
                column.set_name(meta.column_name().as_ref().unwrap().to_string());
                column.set_isKey(meta.key());
                column.set_mysqlType(meta.column_type().as_ref().unwrap().as_str().to_string());
                column.set_index(0);
                column.set_isNull(false);
                buffer.next_value_is_binary(column.get_name().to_string(), i, info.kind(), info.meta(), false);

                let java_type = buffer.java_type();
                if buffer.f_null() {
                    column.set_isNull(true);
                } else {
                    let value = buffer.value();
                    match java_type {
                        Types::INTEGER
                        | Types::TINYINT
                        | Types::SMALLINT
                        | Types::BIGINT => {
                            column.set_value(value.value());
                        }
                        Types::REAL
                        | Types::DOUBLE => {
                            column.set_value(value.value());
                        }
                        Types::BIT => {
                            column.set_value(value.value());
                        }
                        Types::DECIMAL => {
                            column.set_value(value.value());
                        }
                        Types::TIMESTAMP |
                        Types::TIME |
                        Types::DATE => {
                            column.set_value(value.value())
                        }
                        Types::BINARY |
                        Types::VARBINARY |
                        Types::LONGVARBINARY => {
                           todo!()
                        }
                        Types::CHAR |
                        Types::VARCHAR => {
                            column.set_value(value.value());
                        }
                        _ => {
                            column.set_value(value.value())
                        }
                    }
                }
            }

            row_data_list.push(column);
            field_meta = Option::None;
            i += 1;
        }

        row_data.set_afterColumns(row_data_list);
        false
    }

    fn parse_table_map_event(event: &mut TableMapLogEvent) {
        let charset_dbname = ISO_8859_1.decode(event.dbname().as_ref().unwrap().as_bytes(), DecoderTrap::Strict).unwrap();
        event.set_dbname(Option::Some(charset_dbname));
        let charset_tbname = ISO_8859_1.decode(event.tblname().as_ref().unwrap().as_bytes(), DecoderTrap::Replace).unwrap();
        event.set_tblname(Option::Some(charset_tbname));
    }
    fn parse_xid_event(event: &XidLogEvent) -> Option<Entry> {
        let mut end = TransactionEnd::new();
        end.set_transactionId(event.xid().to_string());
        let header = Self::create_header(event.event().header(), Option::Some("".into()), Option::Some("".into()), None);
        return Option::Some(Self::create_entry(header, EntryType::TRANSACTIONEND, end.write_to_bytes().unwrap()));
    }

    fn parse_query_event(event: &QueryLogEvent) -> Option<Entry> {
        let query = event.query().as_ref().unwrap();

        if query.starts_with_ignore_case(Self::XA_START) {
            let mut begin = TransactionBegin::new();
            begin.set_threadId(event.session_id() as i64);
            let mut list = RepeatedField::new();
            list.push(Self::create_special_pair(Self::XA_TYPE.into(), Self::XA_START.into()));
            list.push(Self::create_special_pair(Self::XA_XID.into(), Self::get_xa_xid(query.into(), Self::XA_START.into())));
            begin.set_props(list);
            let header = Self::create_header(event.event().header(), Option::Some("".into()), Option::Some("".into()), None);
            return Option::Some(Self::create_entry(header, EntryType::TRANSACTIONBEGIN, begin.write_to_bytes().unwrap()));
        } else if query.starts_with_ignore_case(Self::XA_END) {
            let mut end = TransactionEnd::new();
            end.set_transactionId("0".into());
            let mut list = RepeatedField::new();
            list.push(Self::create_special_pair(Self::XA_TYPE.into(), Self::XA_END.into()));
            list.push(Self::create_special_pair(Self::XA_XID.into(), Self::get_xa_xid(query.into(), Self::XA_END.into())));
            end.set_props(list);
            let header = Self::create_header(event.event().header(),
                                             Option::Some("".into()), Option::Some("".into()), None,
            );
            return Option::Some(Self::create_entry(header, EntryType::TRANSACTIONEND,
                                                   end.write_to_bytes().unwrap()));
        } else if query.starts_with_ignore_case(Self::XA_COMMIT) {
            let mut change = RowChange::new();
            change.set_sql(query.into());
            let mut list = RepeatedField::new();
            list.push(Self::create_special_pair(Self::XA_TYPE.into(), Self::XA_COMMIT.into()));
            list.push(Self::create_special_pair(Self::XA_XID.into(), Self::get_xa_xid(query.into(), Self::XA_COMMIT.into())));
            change.set_props(list);
            change.set_eventType(EventType::XACOMMIT);
            let header = Self::create_header(event.event().header(), Option::Some("".into()), Option::Some("".into()), Option::Some(EventType::XACOMMIT));
            return Option::Some(Self::create_entry(header, EntryType::ROWDATA, change.write_to_bytes().unwrap()));
        } else if query.starts_with_ignore_case(Self::XA_ROLLBACK) {
            let mut change = RowChange::new();
            change.set_sql(query.into());
            let mut list = RepeatedField::new();
            list.push(Self::create_special_pair(Self::XA_TYPE.into(), Self::XA_ROLLBACK.into()));
            list.push(Self::create_special_pair(Self::XA_XID.into(), Self::get_xa_xid(query.into(), Self::XA_ROLLBACK.into())));
            change.set_props(list);
            change.set_eventType(EventType::XAROLLBACK);
            let header = Self::create_header(event.event().header(), Option::Some("".into()), Option::Some("".into()), EventType::XAROLLBACK.into());
            return Option::Some(Self::create_entry(header, EntryType::ROWDATA, change.write_to_bytes().unwrap()));
        } else if query.starts_with_ignore_case(Self::BEGIN) {
            let mut begin = TransactionBegin::new();
            begin.set_threadId(event.session_id() as i64);
            let header = Self::create_header(event.event().header(), Option::Some("".into()), Option::Some("".into()), None);
            return Option::Some(Self::create_entry(header.clone(), EntryType::TRANSACTIONBEGIN, begin.write_to_bytes().unwrap()));
        } else if query.starts_with_ignore_case(Self::COMMIT) {
            let mut end = TransactionEnd::new();
            end.set_transactionId("0".into());
            let header = Self::create_header(event.event().header(), Option::Some("".into()), Option::Some("".into()), None);
            return Option::Some(Self::create_entry(header, EntryType::TRANSACTIONEND, end.write_to_bytes().unwrap()));
        } else {}
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
            let pair = Self::create_special_pair("rowsCount".into(), rows_count.to_string());
            list.push(pair);
        }

        if list.len() != 0 {
            header.set_props(list);
        }
        header
    }
}