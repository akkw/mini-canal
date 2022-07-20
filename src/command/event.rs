use std::collections::HashMap;
use std::env::var;
use std::f32::consts::E;
use std::fmt::{Display, Formatter, write};
use std::iter::Map;
use std::mem::take;
use std::ops::Add;
use std::os::unix::raw::gid_t;
use bigdecimal::BigDecimal;
use bit_set::BitSet;
use chrono::format::parse;
use num::BigInt;
use uuid::Uuid;
use crate::command::{get_i64, HeaderPacket};
use crate::command::event::LogEvent::{FormatDescriptionLog, XidLog};
use crate::instance::log_buffer::LogBuffer;


/* Enumeration type for the different types of log events. */
pub const UNKNOWN_EVENT: u8 = 0;
pub const START_EVENT_V3: usize = 1;
pub const QUERY_EVENT: usize = 2;
pub const STOP_EVENT: usize = 3;
pub const ROTATE_EVENT: usize = 4;
pub const INTVAR_EVENT: usize = 5;
pub const LOAD_EVENT: usize = 6;
pub const SLAVE_EVENT: usize = 7;
pub const CREATE_FILE_EVENT: usize = 8;
pub const APPEND_BLOCK_EVENT: usize = 9;
pub const EXEC_LOAD_EVENT: usize = 10;
pub const DELETE_FILE_EVENT: usize = 11;

/*
     * 3 is MySQL 4.x; 4 is MySQL 5.0.0. Compared to version 3, version 4 has: -
     * a different Start_log_event, which includes info about the binary log
     * (sizes of headers); this info is included for better compatibility if the
     * master's MySQL version is different from the slave's. - all events have a
     * unique ID (the triplet (server_id, timestamp at server start, other) to
     * be sure an event is not executed more than once in a multimaster setup,
     * example: M1 / \ v v M2 M3 \ / v v S if a query is run on M1, it will
     * arrive twice on S, so we need that S remembers the last unique ID it has
     * processed, to compare and know if the event should be skipped or not.
     * Example of ID: we already have the server id (4 bytes), plus:
     * timestamp_when_the_master_started (4 bytes), a counter (a sequence number
     * which increments every time we write an event to the binlog) (3 bytes).
     * Q: how do we handle when the counter is overflowed and restarts from 0 ?
     * - Query and Load (Create or Execute) events may have a more precise
     * timestamp (with microseconds), number of matched/affected/warnings rows
     * and fields of session variables: SQL_MODE, FOREIGN_KEY_CHECKS,
     * UNIQUE_CHECKS, SQL_AUTO_IS_NULL, the collations and charsets, the
     * PASSWORD() version (old/new/...).
     */

pub const BINLOG_VERSION: u8 = 4;

/* Default 5.0 server version */
pub const SERVER_VERSION: &str = "5.0";

/**
 * Event header offsets; these point to places inside the fixed header.
 */
pub const EVENT_TYPE_OFFSET: u8 = 4;
pub const SERVER_ID_OFFSET: u8 = 5;
pub const EVENT_LEN_OFFSET: u8 = 9;
pub const LOG_POS_OFFSET: u8 = 13;
pub const FLAGS_OFFSET: u8 = 17;

/* event-specific post-header sizes */
// where 3.23, 4.x and 5.0 agree
pub const QUERY_HEADER_MINIMAL_LEN: u8 = 4 + 4 + 1 + 2;
// where 5.0 differs: 2 for len of N-bytes vars.
pub const QUERY_HEADER_LEN: u8 = QUERY_HEADER_MINIMAL_LEN + 2;


/**
 * NEW_LOAD_EVENT is like LOAD_EVENT except that it has a longer sql_ex,
 * allowing multibyte TERMINATED BY etc; both types share the same class
 * (Load_log_event)
 */
pub const NEW_LOAD_EVENT: usize = 12;
pub const RAND_EVENT: usize = 13;
pub const USER_VAR_EVENT: usize = 14;
pub const FORMAT_DESCRIPTION_EVENT: usize = 15;
pub const XID_EVENT: usize = 16;
pub const BEGIN_LOAD_QUERY_EVENT: usize = 17;
pub const EXECUTE_LOAD_QUERY_EVENT: usize = 18;
pub const TABLE_MAP_EVENT: usize = 19;

/**
 * These event numbers were used for 5.1.0 to 5.1.15 and are therefore
 * obsolete.
 */
pub const PRE_GA_WRITE_ROWS_EVENT: usize = 20;
pub const PRE_GA_UPDATE_ROWS_EVENT: usize = 21;
pub const PRE_GA_DELETE_ROWS_EVENT: usize = 22;

/**
 * These event numbers are used from 5.1.16 and forward
 */
pub const WRITE_ROWS_EVENT_V1: usize = 23;
pub const UPDATE_ROWS_EVENT_V1: usize = 24;
pub const DELETE_ROWS_EVENT_V1: usize = 25;

/**
 * Something out of the ordinary happened on the master
 */
pub const INCIDENT_EVENT: usize = 26;

/**
 * Heartbeat event to be send by master at its idle time to ensure master's
 * online status to slave
 */
pub const HEARTBEAT_LOG_EVENT: usize = 27;

/**
 * In some situations, it is necessary to send over ignorable data to the
 * slave: data that a slave can handle in  there is code for handling
 * it, but which can be ignored if it is not recognized.
 */
pub const IGNORABLE_LOG_EVENT: usize = 28;
pub const ROWS_QUERY_LOG_EVENT: usize = 29;

/** Version 2 of the Row events */
pub const WRITE_ROWS_EVENT: usize = 30;
pub const UPDATE_ROWS_EVENT: usize = 31;
pub const DELETE_ROWS_EVENT: usize = 32;
pub const GTID_LOG_EVENT: usize = 33;
pub const ANONYMOUS_GTID_LOG_EVENT: usize = 34;

pub const PREVIOUS_GTIDS_LOG_EVENT: usize = 35;

/* MySQL 5.7 events */
pub const TRANSACTION_CONTEXT_EVENT: usize = 36;

pub const VIEW_CHANGE_EVENT: usize = 37;

/* Prepared XA transaction terminal event similar to Xid */
pub const XA_PREPARE_LOG_EVENT: usize = 38;

/**
 * Extension of UPDATE_ROWS_EVENT, allowing partial values according to
 * binlog_row_value_options.
 */
pub const PARTIAL_UPDATE_ROWS_EVENT: usize = 39;

/* mysql 8.0.20 */
pub const TRANSACTION_PAYLOAD_EVENT: usize = 40;

pub const MYSQL_ENUM_END_EVENT: usize = 41;

// mariaDb 5.5.34
/* New MySQL/Sun events are to be added right above this comment */
pub const MYSQL_EVENTS_END: usize = 49;

pub const MARIA_EVENTS_BEGIN: usize = 160;
/* New Maria event numbers start from here */
pub const ANNOTATE_ROWS_EVENT: usize = 160;
/*
 * Binlog checkpoint event. Used for XA crash recovery on the master, not
 * used in replication. A binlog checkpoint event specifies a binlog file
 * such that XA crash recovery can start from that file - and it is
 * guaranteed to find all XIDs that are prepared in storage engines but not
 * yet committed.
 */
pub const BINLOG_CHECKPOINT_EVENT: usize = 161;
/*
 * Gtid event. For global transaction ID, used to start a new event group,
 * instead of the old BEGIN query event, and also to mark stand-alone
 * events.
 */
pub const GTID_EVENT: usize = 162;
/*
 * Gtid list event. Logged at the start of every binlog, to record the
 * current replication state. This consists of the last GTID seen for each
 * replication domain.
 */
pub const GTID_LIST_EVENT: usize = 163;

pub const START_ENCRYPTION_EVENT: usize = 164;

/** end marker */
pub const ENUM_END_EVENT: usize = 165;

/**
 * 1 byte length, 1 byte format Length is total length in bytes, including 2
 * byte header Length values 0 and 1 are currently invalid and reserved.
 */
pub const EXTRA_ROW_INFO_LEN_OFFSET: u8 = 0;
pub const EXTRA_ROW_INFO_FORMAT_OFFSET: u8 = 1;
pub const EXTRA_ROW_INFO_HDR_BYTES: u8 = 2;
pub const EXTRA_ROW_INFO_MAX_PAYLOAD: u8 = 255 - EXTRA_ROW_INFO_HDR_BYTES;

// Events are without checksum though its generator
pub const BINLOG_CHECKSUM_ALG_OFF: u8 = 0;
// is checksum-capable New Master (NM).
// CRC32 of zlib algorithm.
pub const BINLOG_CHECKSUM_ALG_CRC32: u8 = 1;
// the cut line: valid alg range is [1, 0x7f].
pub const BINLOG_CHECKSUM_ALG_ENUM_END: u8 = 2;
// special value to tag undetermined yet checksum
pub const BINLOG_CHECKSUM_ALG_UNDEF: u8 = 255;
// or events from checksum-unaware servers

pub const CHECKSUM_CRC32_SIGNATURE_LEN: u8 = 4;
pub const BINLOG_CHECKSUM_ALG_DESC_LEN: u8 = 1;
/**
 * defined statically while there is just one alg implemented
 */
pub const BINLOG_CHECKSUM_LEN: u8 = CHECKSUM_CRC32_SIGNATURE_LEN;

/* MySQL or old MariaDB slave with no announced capability. */
pub const MARIA_SLAVE_CAPABILITY_UNKNOWN: u8 = 0;

/* MariaDB >= 5.3, which understands ANNOTATE_ROWS_EVENT. */
pub const MARIA_SLAVE_CAPABILITY_ANNOTATE: u8 = 1;
/*
 * MariaDB >= 5.5. This version has the capability to tolerate events
 * omitted from the binlog stream without breaking replication (MySQL slaves
 * fail because they mis-compute the offsets into the master's binlog).
 */
pub const MARIA_SLAVE_CAPABILITY_TOLERATE_HOLES: u8 = 2;
/* MariaDB >= 10.0, which knows about binlog_checkpoint_log_event. */
pub const MARIA_SLAVE_CAPABILITY_BINLOG_CHECKPOINT: u8 = 3;
/* MariaDB >= 10.0.1, which knows about global transaction id events. */
pub const MARIA_SLAVE_CAPABILITY_GTID: u8 = 4;

/* Our capability. */
pub const MARIA_SLAVE_CAPABILITY_MINE: u8 = MARIA_SLAVE_CAPABILITY_GTID;

/**
 * For an event, 'e', carrying a type code, that a slave, 's', does not
 * recognize, 's' will check 'e' for LOG_EVENT_IGNORABLE_F, and if the flag
 * is set, then 'e' is ignored. Otherwise, 's' acknowledges that it has
 * found an unknown event in the relay log.
 */
pub const LOG_EVENT_IGNORABLE_F: u8 = 0x80;

/** enum_field_types */
pub const MYSQL_TYPE_DECIMAL: u8 = 0;
pub const MYSQL_TYPE_TINY: u8 = 1;
pub const MYSQL_TYPE_SHORT: u8 = 2;
pub const MYSQL_TYPE_LONG: u8 = 3;
pub const MYSQL_TYPE_FLOAT: u8 = 4;
pub const MYSQL_TYPE_DOUBLE: u8 = 5;
pub const MYSQL_TYPE_NULL: u8 = 6;
pub const MYSQL_TYPE_TIMESTAMP: u8 = 7;
pub const MYSQL_TYPE_LONGLONG: u8 = 8;
pub const MYSQL_TYPE_INT24: u8 = 9;
pub const MYSQL_TYPE_DATE: u8 = 10;
pub const MYSQL_TYPE_TIME: u8 = 11;
pub const MYSQL_TYPE_DATETIME: u8 = 12;
pub const MYSQL_TYPE_YEAR: u8 = 13;
pub const MYSQL_TYPE_NEWDATE: u8 = 14;
pub const MYSQL_TYPE_VARCHAR: u8 = 15;
pub const MYSQL_TYPE_BIT: u8 = 16;
pub const MYSQL_TYPE_TIMESTAMP2: u8 = 17;
pub const MYSQL_TYPE_DATETIME2: u8 = 18;
pub const MYSQL_TYPE_TIME2: u8 = 19;
pub const MYSQL_TYPE_TYPED_ARRAY: u8 = 20;
pub const MYSQL_TYPE_INVALID: u8 = 243;
pub const MYSQL_TYPE_BOOL: u8 = 244;
pub const MYSQL_TYPE_JSON: u8 = 245;
pub const MYSQL_TYPE_NEWDECIMAL: u8 = 246;
pub const MYSQL_TYPE_ENUM: u8 = 247;
pub const MYSQL_TYPE_SET: u8 = 248;
pub const MYSQL_TYPE_TINY_BLOB: u8 = 249;
pub const MYSQL_TYPE_MEDIUM_BLOB: u8 = 250;
pub const MYSQL_TYPE_LONG_BLOB: u8 = 251;
pub const MYSQL_TYPE_BLOB: u8 = 252;
pub const MYSQL_TYPE_VAR_STRING: u8 = 253;
pub const MYSQL_TYPE_STRING: u8 = 254;
pub const MYSQL_TYPE_GEOMETRY: u8 = 255;


pub struct LogHeader {
    kind: usize,
    log_pos: u32,
    when: u32,
    event_len: usize,
    server_id: u32,
    flags: u16,
    checksum_alg: u8,
    crc: u32,
    log_file_name: Option<String>,
    gtid_map: HashMap<String, String>,
}

impl Clone for LogHeader {
    fn clone(&self) -> Self {
        LogHeader {
            kind: self.kind,
            log_pos: self.log_pos,
            when: self.when,
            event_len: self.event_len,
            server_id: self.server_id,
            flags: self.flags,
            checksum_alg: self.checksum_alg,
            crc: self.crc,
            log_file_name: self.log_file_name.clone(),
            gtid_map: self.gtid_map.clone(),
        }
    }
}

impl LogHeader {
    pub fn new() -> Self {
        Self { kind: 0, log_pos: 0, when: 0, event_len: 0, server_id: 0, flags: 0, checksum_alg: 0, crc: 0, log_file_name: Option::Some(String::new()), gtid_map: Default::default() }
    }

    pub fn from_buffer(buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Option<LogHeader> {
        let mut header = LogHeader {
            when: 0,
            kind: 0,
            server_id: 0,
            event_len: 0,
            log_pos: 0,
            flags: 0,
            checksum_alg: 0,
            crc: 0,
            log_file_name: None,
            gtid_map: Default::default(),
        };

        header.when = buffer.get_uint32().ok()?;
        header.kind = buffer.get_uint8().ok()? as usize;
        header.server_id = buffer.get_uint32().ok()?;
        header.event_len = buffer.get_uint32().ok()? as usize;

        if description_event.binlog_version == 1 {
            header.log_pos = 0;
            header.flags = 0;
            return Option::Some(header);
        }
        /* 4.0 or newer */
        header.log_pos = buffer.get_uint32().ok()?;
        /*
         * If the log is 4.0 (so here it can only be a 4.0 relay log read by the
         * SQL thread or a 4.0 master binlog read by the I/O thread), log_pos is
         * the beginning of the event: we transform it into the end of the
         * event, which is more useful. But how do you know that the log is 4.0:
         * you know it if description_event is version 3 *and* you are not
         * reading a Format_desc (remember that mysqlbinlog starts by assuming
         * that 5.0 logs are in 4.0 format, until it finds a Format_desc).
         */
        if description_event.binlog_version == 3 && header.kind < FORMAT_DESCRIPTION_EVENT && header.log_pos != 0 {
            /*
             * If log_pos=0, don't change it. log_pos==0 is a marker to mean
             * "don't change rli->group_master_log_pos" (see
             * inc_group_relay_log_pos()). As it is unreal log_pos, adding the
             * event len's is nonsense. For example, a fake Rotate event should
             * not have its log_pos (which is 0) changed or it will modify
             * Exec_master_log_pos in SHOW SLAVE STATUS, displaying a nonsense
             * value of (a non-zero offset which does not exist in the master's
             * binlog, so which will cause problems if the user uses this value
             * in CHANGE MASTER).
             */
            header.log_pos += header.event_len as u32;
        }

        header.flags = buffer.get_uint16().ok()?;
        if header.kind == FORMAT_DESCRIPTION_EVENT || header.kind == ROTATE_EVENT {
            /*
            * These events always have a header which stops here (i.e. their
            * header is FROZEN).
            *
            * Initialization to zero of all other Log_event members as they're
            * not specified. Currently there are no such members; in the future
            * there will be an event UID (but Format_description and Rotate
            * don't need this UID, as they are not propagated through
            * --log-slave-updates (remember the UID is used to not play a query
            * twice when you have two masters which are slaves of a 3rd
            * master). Then we are done.
            */

            if header.kind == FORMAT_DESCRIPTION_EVENT {
                let common_header_len = buffer.get_uint8_pos(
                    (FormatDescriptionLogEvent::LOG_EVENT_MINIMAL_HEADER_LEN + FormatDescriptionLogEvent::ST_COMMON_HEADER_LEN_OFFSET) as usize
                ).ok()? as usize;

                buffer.up_position(common_header_len + FormatDescriptionLogEvent::ST_SERVER_VER_OFFSET as usize);
                let server_version = buffer.get_fix_string_len(FormatDescriptionLogEvent::ST_SERVER_VER_LEN)?;
                let mut version_split = [0, 0, 0];
                FormatDescriptionLogEvent::do_server_version_split(&server_version, &mut version_split);
                header.checksum_alg = BINLOG_CHECKSUM_ALG_UNDEF;

                if FormatDescriptionLogEvent::version_product(&mut version_split) >= FormatDescriptionLogEvent::CHECKSUM_VERSION_PRODUCT {
                    buffer.up_position(header.event_len - (BINLOG_CHECKSUM_LEN - BINLOG_CHECKSUM_ALG_DESC_LEN) as usize);
                    header.checksum_alg = buffer.get_uint8().ok()?
                }

                header.processCheckSum(buffer);
            }
            return Option::Some(header);
        }
        header.checksum_alg = description_event.start_log_event_v3.event.header.checksum_alg;
        header.processCheckSum(buffer);
        Option::Some(header)
    }


    fn processCheckSum(&mut self, buffer: &mut LogBuffer) {
        if self.checksum_alg != BINLOG_CHECKSUM_ALG_OFF && self.checksum_alg != BINLOG_CHECKSUM_ALG_UNDEF {
            self.crc = buffer.get_uint32_pos(self.event_len - BINLOG_CHECKSUM_ALG_DESC_LEN as usize).unwrap();
        }
    }
    pub fn from_kind(kind: usize) -> LogHeader {
        LogHeader {
            kind,
            log_pos: 0,
            when: 0,
            event_len: 0,
            server_id: 0,
            flags: 0,
            checksum_alg: 0,
            crc: 0,
            log_file_name: Option::None,
            gtid_map: HashMap::<String, String>::new(),
        }
    }


    pub fn kind(&self) -> usize {
        self.kind
    }
    pub fn log_pos(&self) -> u32 {
        self.log_pos
    }
    pub fn when(&self) -> u32 {
        self.when
    }
    pub fn event_len(&self) -> usize {
        self.event_len
    }
    pub fn server_id(&self) -> u32 {
        self.server_id
    }
    pub fn flags(&self) -> u16 {
        self.flags
    }
    pub fn checksum_alg(&self) -> u8 {
        self.checksum_alg
    }
    pub fn crc(&self) -> u32 {
        self.crc
    }
    pub fn log_file_name(&self) -> &Option<String> {
        &self.log_file_name
    }
    pub fn gtid_map(&self) -> &HashMap<String, String> {
        &self.gtid_map
    }
    pub fn set_kind(&mut self, kind: usize) {
        self.kind = kind;
    }
    pub fn set_log_pos(&mut self, log_pos: u32) {
        self.log_pos = log_pos;
    }
    pub fn set_when(&mut self, when: u32) {
        self.when = when;
    }
    pub fn set_event_len(&mut self, event_len: usize) {
        self.event_len = event_len;
    }
    pub fn set_server_id(&mut self, server_id: u32) {
        self.server_id = server_id;
    }
    pub fn set_flags(&mut self, flags: u16) {
        self.flags = flags;
    }
    pub fn set_checksum_alg(&mut self, checksum_alg: u8) {
        self.checksum_alg = checksum_alg;
    }
    pub fn set_crc(&mut self, crc: u32) {
        self.crc = crc;
    }
    pub fn set_log_file_name(&mut self, log_file_name: Option<String>) {
        self.log_file_name = log_file_name;
    }
    pub fn set_gtid_map(&mut self, gtid_map: HashMap<String, String>) {
        self.gtid_map = gtid_map;
    }
}


pub struct Event {
    header: LogHeader,
    semival: u32,
}

impl Clone for Event {
    fn clone(&self) -> Self {
        Event {
            header: self.header.clone(),
            semival: self.semival,
        }
    }
}

impl Event {
    fn get_type_name(t: usize) -> String {
        match t {
            START_EVENT_V3 => String::from("Start_v3"),
            STOP_EVENT => String::from("Stop"),
            QUERY_EVENT => String::from("Query"),
            ROTATE_EVENT => String::from("Rotate"),
            INTVAR_EVENT => String::from("Intvar"),
            LOAD_EVENT => String::from("Load"),
            NEW_LOAD_EVENT => String::from("New_load"),
            SLAVE_EVENT => String::from("Slave"),
            CREATE_FILE_EVENT => String::from("Create_file"),
            APPEND_BLOCK_EVENT => String::from("Append_block"),
            DELETE_FILE_EVENT => String::from("Delete_file"),
            EXEC_LOAD_EVENT => String::from("Exec_load"),
            RAND_EVENT => String::from("RAND"),
            XID_EVENT => String::from("Xid"),
            USER_VAR_EVENT => String::from("User var"),
            FORMAT_DESCRIPTION_EVENT => String::from("Format_desc"),
            TABLE_MAP_EVENT => String::from("Table_map"),
            PRE_GA_WRITE_ROWS_EVENT => String::from("Write_rows_event_old"),

            PRE_GA_UPDATE_ROWS_EVENT => String::from("Update_rows_event_old"),
            PRE_GA_DELETE_ROWS_EVENT => String::from("Delete_rows_event_old"),
            WRITE_ROWS_EVENT_V1 => String::from("Write_rows_v1"),
            UPDATE_ROWS_EVENT_V1 => String::from("Update_rows_v1"),
            DELETE_ROWS_EVENT_V1 => String::from("Delete_rows_v1"),
            BEGIN_LOAD_QUERY_EVENT => String::from("Begin_load_query"),
            EXECUTE_LOAD_QUERY_EVENT => String::from("Execute_load_query"),
            INCIDENT_EVENT => String::from("Incident"),
            HEARTBEAT_LOG_EVENT => String::from("Heartbeat"),
            IGNORABLE_LOG_EVENT => String::from("Ignorable"),

            ROWS_QUERY_LOG_EVENT => String::from("Rows_query"),
            WRITE_ROWS_EVENT => String::from("Write_rows"),
            UPDATE_ROWS_EVENT => String::from("Update_rows"),
            DELETE_ROWS_EVENT => String::from("Delete_rows"),
            GTID_LOG_EVENT => String::from("Gtid"),
            ANONYMOUS_GTID_LOG_EVENT => String::from("Anonymous_Gtid"),
            PREVIOUS_GTIDS_LOG_EVENT => String::from("Previous_gtids"),
            PARTIAL_UPDATE_ROWS_EVENT => String::from("Update_rows_partial"),
            TRANSACTION_CONTEXT_EVENT => String::from("Transaction_context"),
            VIEW_CHANGE_EVENT => String::from("view_change"),
            XA_PREPARE_LOG_EVENT => String::from("Xa_prepare"),
            TRANSACTION_PAYLOAD_EVENT => String::from("transaction_payload"),
            _ => format!("Unknown type=> {}", t)
        }
    }
    pub fn header(&self) -> &LogHeader {
        &self.header
    }
    pub fn semival(&self) -> u32 {
        self.semival
    }

    pub fn from(&mut self, header: LogHeader) {
        self.header = header;
    }
    pub fn set_semival(&mut self, semival: u32) {
        self.semival = semival;
    }
    pub fn get_event_len(&self) -> usize {
        self.header.event_len
    }
    pub fn get_server_id(&self) -> u32 {
        self.header.server_id
    }

    pub fn get_log_pos(&self) -> u32 {
        self.header.log_pos
    }

    pub fn get_when(&self) -> u32 {
        self.header.when
    }
    // pub fn new() -> Self {
    //     Self { header: (), semival: () }
    // }
    pub fn new() -> Self {
        Self { header: LogHeader::new(), semival: 0 }
    }
}


pub struct AppendBlockLogEvent {
    event: Event,
    block_buf: LogBuffer,
    block_len: usize,
    filed_id: u32,
}

impl AppendBlockLogEvent {
    const AB_FILE_ID_OFFSET: usize = 0;


    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> AppendBlockLogEvent {
        let mut event = AppendBlockLogEvent {
            event: Event {
                header: (*header).clone(),
                semival: 0,
            },
            block_buf: LogBuffer::new(),
            block_len: 0,
            filed_id: 0,
        };
        let common_header_len = description_event.common_header_len;
        let post_header_len = description_event.post_header_len[header.kind - 1] as usize;
        let total_header_len = common_header_len + post_header_len;
        buffer.up_position(common_header_len + Self::AB_FILE_ID_OFFSET);
        event.filed_id = buffer.get_uint32().unwrap();
        buffer.up_position(post_header_len);
        event.block_len = buffer.limit() - total_header_len;
        event.block_buf = buffer.duplicate_len(event.block_len).unwrap();
        event
    }

    pub fn get_data(&mut self) -> Box<[u8]> {
        self.block_buf.get_data()
    }
    pub fn block_buf(&self) -> &LogBuffer {
        &self.block_buf
    }
    pub fn filed_id(&self) -> u32 {
        self.filed_id
    }
    pub fn new() -> Self {
        Self { event: Event::new(), block_buf: LogBuffer::new(), block_len: 0, filed_id: 0 }
    }
}

pub struct BeginLoadQueryLogEvent {
    append_block_log_event: AppendBlockLogEvent,
}

impl BeginLoadQueryLogEvent {
    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> BeginLoadQueryLogEvent {
        BeginLoadQueryLogEvent {
            append_block_log_event: AppendBlockLogEvent::from(header, buffer, description_event),
        }
    }
}

pub struct CreateFileLogEvent {
    event: Event,
    load_log_event: LoadLogEvent,
    block_buf: LogBuffer,
    block_len: usize,
    filed_id: u32,
    inited_from_old: bool,
}

impl CreateFileLogEvent {
    const CF_FILE_ID_OFFSET: usize = 0;
    const CF_DATA_OFFSET: u8 = FormatDescriptionLogEvent::CREATE_FILE_HEADER_LEN;

    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Self {
        let mut event = CreateFileLogEvent {
            event: Event::new(),
            load_log_event: LoadLogEvent::from(header, buffer, description_event),
            block_buf: LogBuffer::new(),
            block_len: 0,
            filed_id: 0,
            inited_from_old: false,
        };

        let header_len = description_event.common_header_len;
        let load_header_len = description_event.post_header_len[LOAD_EVENT - 1] as usize;
        let create_file_header_len = description_event.post_header_len[CREATE_FILE_EVENT - 1] as usize;
        let offset = if header.kind == LOAD_EVENT { load_header_len + header_len } else { header_len + load_header_len + create_file_header_len };
        event.load_log_event.copy_log_event(buffer, offset, description_event);
        if description_event.binlog_version != 1 {
            event.filed_id = buffer.get_uint32_pos(header_len + load_header_len + Self::CF_FILE_ID_OFFSET).unwrap();
            event.block_len = buffer.limit() - buffer.position();
            event.block_buf = buffer.duplicate_len(event.block_len).unwrap();
        } else {
            event.inited_from_old = true
        }
        event
    }


    pub fn block_buf(&self) -> &LogBuffer {
        &self.block_buf
    }
    pub fn filed_id(&self) -> u32 {
        self.filed_id
    }

    pub fn get_data(&mut self) -> Box<[u8]> {
        self.block_buf.get_data()
    }
}

pub struct DeleteFileLogEvent {
    event: Event,
    filed_id: u32,
}

impl DeleteFileLogEvent {
    const DF_FILE_ID_OFFSET: usize = 0;
    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Self {
        let mut event = DeleteFileLogEvent {
            event: Event::new(),
            filed_id: 0,
        };
        let common_header_len = description_event.common_header_len;
        buffer.up_position(common_header_len + DeleteFileLogEvent::DF_FILE_ID_OFFSET);
        event.filed_id = buffer.get_uint32().unwrap();
        event
    }


    pub fn filed_id(&self) -> u32 {
        self.filed_id
    }
}

pub struct DeleteRowsLogEvent {
    rows_log_event: RowsLogEvent,
}

impl DeleteRowsLogEvent {
    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Self {
        let event = DeleteRowsLogEvent { rows_log_event: RowsLogEvent::from(header, buffer, description_event, false) };
        event
    }
}

pub struct ExecuteLoadLogEvent {
    event: Event,
    file_id: u32,
}

impl ExecuteLoadLogEvent {
    const EL_FILE_ID_OFFSET: u8 = 0;
    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Self {
        let mut event = ExecuteLoadLogEvent {
            event: Event::new(),
            file_id: 0,
        };
        event.event.header = (*header).clone();
        let common_header_len = description_event.common_header_len;
        buffer.up_position(common_header_len + Self::EL_FILE_ID_OFFSET as usize);
        event.file_id = buffer.get_uint32().unwrap();
        event
    }


    pub fn file_id(&self) -> u32 {
        self.file_id
    }
}

pub struct ExecuteLoadQueryLogEvent {
    query_log_event: QueryLogEvent,
    file_id: u32,
    fn_pos_start: usize,
    fn_pos_end: usize,
    dup_hand_ling: u8,
}

impl ExecuteLoadQueryLogEvent {
    const LOAD_DUP_ERROR: u8 = 0;
    const LOAD_DUP_IGNORE: u8 = Self::LOAD_DUP_ERROR + 1;
    const LOAD_DUP_REPLACE: u8 = Self::LOAD_DUP_IGNORE + 1;
    const ELQ_FILE_ID_OFFSET: u8 = QUERY_HEADER_LEN;
    const ELQ_FN_POS_START_OFFSET: u8 = Self::ELQ_FILE_ID_OFFSET + 4;
    const ELQ_FN_POS_END_OFFSET: u8 = Self::ELQ_FILE_ID_OFFSET + 8;
    const ELQ_DUP_HANDLING_OFFSET: u8 = Self::ELQ_FILE_ID_OFFSET + 12;

    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> StringResult<Self> {
        let mut event = ExecuteLoadQueryLogEvent {
            query_log_event: QueryLogEvent::from(header, buffer, description_event)?,
            file_id: 0,
            fn_pos_start: 0,
            fn_pos_end: 0,
            dup_hand_ling: 0,
        };
        buffer.up_position(description_event.common_header_len + Self::ELQ_FILE_ID_OFFSET as usize);
        event.file_id = buffer.get_uint32()?;
        event.fn_pos_start = buffer.get_uint32()? as usize;
        event.dup_hand_ling = buffer.get_uint8()?;
        let len = event.query_log_event.query.as_ref().ok_or(String::from("query is empty"))?.len();
        if event.fn_pos_start > len || event.fn_pos_end > len || event.dup_hand_ling > Self::LOAD_DUP_REPLACE {
            return Result::Err(format!("Invalid ExecuteLoadQueryLogEvent: fn_pos_start={}, fn_pos_end={}, dup_handling={}",
                                       event.fn_pos_start, event.fn_pos_end, event.dup_hand_ling));
        }
        Result::Ok(event)
    }


    pub fn file_id(&self) -> u32 {
        self.file_id
    }
    pub fn fn_pos_start(&self) -> usize {
        self.fn_pos_start
    }
    pub fn fn_pos_end(&self) -> usize {
        self.fn_pos_end
    }
    pub fn get_filename(&self) -> String {
        (&self.query_log_event.query.as_ref().unwrap()[self.fn_pos_start..self.fn_pos_end]).to_string()
    }
}


pub struct FormatDescriptionLogEvent {
    start_log_event_v3: StartLogEventV3,
    binlog_version: u16,
    server_version: Option<String>,
    common_header_len: usize,
    number_of_event_types: usize,
    post_header_len: Box<[u8]>,
    server_version_split: [u8; 3],
}

impl Clone for FormatDescriptionLogEvent {
    fn clone(&self) -> Self {
        FormatDescriptionLogEvent {
            start_log_event_v3: StartLogEventV3::new(),
            binlog_version: self.binlog_version,
            server_version: Option::None,
            common_header_len: self.common_header_len,
            number_of_event_types: self.number_of_event_types,
            post_header_len: self.post_header_len.clone(),
            server_version_split: self.server_version_split,
        }
    }
}


impl FormatDescriptionLogEvent {
    pub const ST_SERVER_VER_LEN: usize = 50;
    pub const ST_BINLOG_VER_OFFSET: u8 = 0;
    pub const ST_SERVER_VER_OFFSET: u8 = 2;
    pub const LOG_EVENT_TYPES: usize = (ENUM_END_EVENT - 1);
    pub const ST_COMMON_HEADER_LEN_OFFSET: u8 = (Self::ST_SERVER_VER_OFFSET + Self::ST_SERVER_VER_LEN as u8 + 4);
    pub const OLD_HEADER_LEN: u8 = 13;
    pub const LOG_EVENT_HEADER_LEN: usize = 19;
    pub const LOG_EVENT_MINIMAL_HEADER_LEN: u8 = 19;
    pub const STOP_HEADER_LEN: u8 = 0;
    pub const LOAD_HEADER_LEN: usize = (4 + 4 + 4 + 1 + 1 + 4);
    pub const SLAVE_HEADER_LEN: u8 = 0;
    pub const START_V3_HEADER_LEN: usize = (2 + Self::ST_SERVER_VER_LEN + 4);
    pub const ROTATE_HEADER_LEN: u8 = 8;
    // th
    pub const INTVAR_HEADER_LEN: u8 = 0;
    pub const CREATE_FILE_HEADER_LEN: u8 = 4;
    pub const APPEND_BLOCK_HEADER_LEN: u8 = 4;
    pub const EXEC_LOAD_HEADER_LEN: u8 = 4;
    pub const DELETE_FILE_HEADER_LEN: usize = 4;
    pub const NEW_LOAD_HEADER_LEN: usize = Self::LOAD_HEADER_LEN;
    pub const RAND_HEADER_LEN: u8 = 0;
    pub const USER_VAR_HEADER_LEN: u8 = 0;
    pub const FORMAT_DESCRIPTION_HEADER_LEN: usize = (Self::START_V3_HEADER_LEN + 1 + Self::LOG_EVENT_TYPES);
    pub const XID_HEADER_LEN: u8 = 0;
    pub const BEGIN_LOAD_QUERY_HEADER_LEN: u8 = Self::APPEND_BLOCK_HEADER_LEN;
    pub const ROWS_HEADER_LEN_V1: u8 = 8;
    pub const TABLE_MAP_HEADER_LEN: u8 = 8;
    pub const EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN: u8 = (4 + 4 + 4 + 1);
    pub const EXECUTE_LOAD_QUERY_HEADER_LEN: u8 = (QUERY_HEADER_LEN + Self::EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN);
    pub const INCIDENT_HEADER_LEN: u8 = 2;
    pub const HEARTBEAT_HEADER_LEN: u8 = 0;
    pub const IGNORABLE_HEADER_LEN: u8 = 0;
    pub const ROWS_HEADER_LEN_V2: u8 = 10;
    pub const TRANSACTION_CONTEXT_HEADER_LEN: u8 = 18;
    pub const VIEW_CHANGE_HEADER_LEN: u8 = 52;
    pub const XA_PREPARE_HEADER_LEN: u8 = 0;
    pub const ANNOTATE_ROWS_HEADER_LEN: u8 = 0;
    pub const BINLOG_CHECKPOINT_HEADER_LEN: u8 = 4;
    pub const GTID_HEADER_LEN: u8 = 19;
    pub const GTID_LIST_HEADER_LEN: u8 = 4;
    pub const START_ENCRYPTION_HEADER_LEN: u8 = 0;
    pub const POST_HEADER_LENGTH: u8 = 11;
    pub const BINLOG_CHECKSUM_ALG_DESC_LEN: u8 = 1;
    pub const CHECKSUM_VERSION_SPLIT: [u8; 3] = [5, 6, 1];
    pub const CHECKSUM_VERSION_PRODUCT: u32 = ((FormatDescriptionLogEvent::CHECKSUM_VERSION_SPLIT[0] as u32 * 256 + FormatDescriptionLogEvent::CHECKSUM_VERSION_SPLIT[1] as u32) * 256 + FormatDescriptionLogEvent::CHECKSUM_VERSION_SPLIT[2] as u32) as u32;
    #[allow(arithmetic_overflow)]
    pub fn from(&mut self, header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Result<FormatDescriptionLogEvent, String> {
        buffer.up_position(description_event.common_header_len);
        let mut event = FormatDescriptionLogEvent {
            start_log_event_v3: StartLogEventV3::new(),
            binlog_version: buffer.get_uint16()?,
            server_version: buffer.get_fix_string_len(FormatDescriptionLogEvent::ST_SERVER_VER_LEN),
            common_header_len: 0,
            number_of_event_types: 0,
            post_header_len: Box::from([]),
            server_version_split: [0, 0, 0],
        };
        event.start_log_event_v3 = StartLogEventV3::from(header, buffer, description_event).unwrap();
        buffer.up_position((FormatDescriptionLogEvent::LOG_EVENT_MINIMAL_HEADER_LEN + FormatDescriptionLogEvent::ST_COMMON_HEADER_LEN_OFFSET) as usize)?;
        self.common_header_len = buffer.get_uint8()? as usize;
        if self.common_header_len < FormatDescriptionLogEvent::OLD_HEADER_LEN as usize {
            return Result::Err(String::from("Format Description event header length is too short"));
        }

        self.number_of_event_types = buffer.limit() - (FormatDescriptionLogEvent::LOG_EVENT_MINIMAL_HEADER_LEN as usize - FormatDescriptionLogEvent::ST_COMMON_HEADER_LEN_OFFSET as usize + 1);

        let mut post_header = vec![];
        for _i in 0..self.number_of_event_types {
            post_header.push(buffer.get_uint8()?);
        }
        self.post_header_len = Box::from(post_header);
        self.calc_server_version_split();
        let calc = self.get_version_product();
        if calc >= FormatDescriptionLogEvent::CHECKSUM_VERSION_PRODUCT {
            self.number_of_event_types -= FormatDescriptionLogEvent::BINLOG_CHECKSUM_ALG_DESC_LEN as usize;
        }

        println!("common_header_len: {} , number_of_event_types:{} ", self.common_header_len, self.number_of_event_types);
        Result::Ok(event)
    }

    pub fn from_binlog_version_binlog_check_sum(binlog_version: u16, binlog_check_num: u8) -> FormatDescriptionLogEvent {
        let mut event = FormatDescriptionLogEvent::from_binlog_version(binlog_version);
        event.start_log_event_v3.event.header.checksum_alg = binlog_check_num;
        event
    }
    pub fn from_binlog_version(binlog_version: u16) -> FormatDescriptionLogEvent {
        let mut event = FormatDescriptionLogEvent {
            start_log_event_v3: StartLogEventV3::from_none(),
            binlog_version,
            server_version: Option::None,
            common_header_len: 0,
            number_of_event_types: 0,
            post_header_len: Box::from([0 as u8, 165]),
            server_version_split: [0, 0, 0],
        };

        match binlog_version {
            4 => {
                event.server_version = Option::Some(SERVER_VERSION.to_string());
                event.common_header_len = Self::LOAD_HEADER_LEN;
                event.number_of_event_types = Self::LOG_EVENT_TYPES;
                event.post_header_len[START_EVENT_V3 - 1] = Self::START_V3_HEADER_LEN as u8;
                event.post_header_len[QUERY_EVENT - 1] = QUERY_HEADER_LEN;
                event.post_header_len[STOP_EVENT - 1] = Self::STOP_HEADER_LEN;
                event.post_header_len[ROTATE_EVENT - 1] = Self::ROTATE_HEADER_LEN;
                event.post_header_len[INTVAR_EVENT - 1] = Self::INTVAR_HEADER_LEN;
                event.post_header_len[LOAD_EVENT - 1] = Self::LOG_EVENT_HEADER_LEN as u8;
                event.post_header_len[SLAVE_EVENT - 1] = Self::STOP_HEADER_LEN;
                event.post_header_len[CREATE_FILE_EVENT - 1] = Self::CREATE_FILE_HEADER_LEN;
                event.post_header_len[APPEND_BLOCK_EVENT - 1] = Self::APPEND_BLOCK_HEADER_LEN;
                event.post_header_len[EXEC_LOAD_EVENT - 1] = Self::EXEC_LOAD_HEADER_LEN;
                event.post_header_len[DELETE_FILE_EVENT - 1] = Self::DELETE_FILE_HEADER_LEN as u8;
                event.post_header_len[NEW_LOAD_EVENT - 1] = Self::NEW_LOAD_HEADER_LEN as u8;
                event.post_header_len[RAND_EVENT - 1] = Self::RAND_HEADER_LEN;
                event.post_header_len[USER_VAR_EVENT - 1] = Self::USER_VAR_HEADER_LEN;
                event.post_header_len[FORMAT_DESCRIPTION_EVENT] = Self::FORMAT_DESCRIPTION_HEADER_LEN as u8;
                event.post_header_len[XID_EVENT - 1] = Self::XID_HEADER_LEN;
                event.post_header_len[BEGIN_LOAD_QUERY_EVENT - 1] = Self::BINLOG_CHECKPOINT_HEADER_LEN;
                event.post_header_len[EXEC_LOAD_EVENT - 1] = Self::EXECUTE_LOAD_QUERY_HEADER_LEN;
                event.post_header_len[TABLE_MAP_EVENT - 1] = Self::TABLE_MAP_HEADER_LEN;
                event.post_header_len[WRITE_ROWS_EVENT - 1] = Self::ROWS_HEADER_LEN_V1;
                event.post_header_len[UPDATE_ROWS_EVENT_V1 - 1] = Self::ROWS_HEADER_LEN_V1;
                event.post_header_len[Self::DELETE_FILE_HEADER_LEN - 1] - Self::ROWS_HEADER_LEN_V1;
                /*
                 * We here have the possibility to simulate a master of before
                 * we changed the table map id to be stored in 6 bytes: when it
                 * was stored in 4 bytes (=> post_header_len was 6). This is
                 * used to test backward compatibility. This code can be removed
                 * after a few months (today is Dec 21st 2005), when we know
                 * that the 4-byte masters are not deployed anymore (check with
                 * Tomas Ulin first!), and the accompanying test
                 * (rpl_row_4_bytes) too.
                 */
                event.post_header_len[HEARTBEAT_LOG_EVENT - 1] = 0;
                event.post_header_len[IGNORABLE_LOG_EVENT - 1] = Self::IGNORABLE_HEADER_LEN;
                event.post_header_len[ROWS_QUERY_LOG_EVENT - 1] = Self::IGNORABLE_HEADER_LEN;
                event.post_header_len[WRITE_ROWS_EVENT - 1] = Self::ROWS_HEADER_LEN_V2;
                event.post_header_len[UPDATE_ROWS_EVENT - 1] = Self::ROWS_HEADER_LEN_V2;
                event.post_header_len[Self::DELETE_FILE_HEADER_LEN - 1] = Self::ROWS_HEADER_LEN_V2;
                event.post_header_len[GTID_LOG_EVENT - 1] = Self::POST_HEADER_LENGTH;
                event.post_header_len[ANONYMOUS_GTID_LOG_EVENT - 1] = Self::POST_HEADER_LENGTH;
                event.post_header_len[PREVIOUS_GTIDS_LOG_EVENT - 1] = Self::IGNORABLE_HEADER_LEN;
                event.post_header_len[TRANSACTION_CONTEXT_EVENT - 1] = Self::TRANSACTION_CONTEXT_HEADER_LEN;
                event.post_header_len[VIEW_CHANGE_EVENT - 1] = Self::VIEW_CHANGE_HEADER_LEN;
                event.post_header_len[XA_PREPARE_LOG_EVENT - 1] = Self::XA_PREPARE_HEADER_LEN;
                event.post_header_len[PARTIAL_UPDATE_ROWS_EVENT - 1] = Self::ROWS_HEADER_LEN_V2;
                event.post_header_len[ANNOTATE_ROWS_EVENT - 1] = Self::ANNOTATE_ROWS_HEADER_LEN;
                event.post_header_len[BINLOG_CHECKPOINT_EVENT - 1] = Self::BINLOG_CHECKPOINT_HEADER_LEN;
                event.post_header_len[GTID_EVENT - 1] = Self::GTID_HEADER_LEN;
                event.post_header_len[GTID_LIST_EVENT - 1] = Self::GTID_LIST_HEADER_LEN;
                event.post_header_len[START_ENCRYPTION_EVENT - 1] = Self::START_ENCRYPTION_HEADER_LEN;
            }
            3 => {
                event.server_version = Option::Some(String::from("4.0"));
                event.common_header_len = Self::LOG_EVENT_MINIMAL_HEADER_LEN as usize;
                event.number_of_event_types = FORMAT_DESCRIPTION_EVENT - 1;
                event.post_header_len[START_EVENT_V3 - 1] = Self::START_V3_HEADER_LEN as u8;
                event.post_header_len[QUERY_EVENT - 1] = QUERY_HEADER_MINIMAL_LEN;
                event.post_header_len[ROTATE_EVENT - 1] = Self::ROTATE_HEADER_LEN;
                event.post_header_len[LOAD_EVENT - 1] = Self::LOAD_HEADER_LEN as u8;
                event.post_header_len[CREATE_FILE_EVENT - 1] = Self::CREATE_FILE_HEADER_LEN;
                event.post_header_len[APPEND_BLOCK_EVENT - 1] = Self::APPEND_BLOCK_HEADER_LEN;
                event.post_header_len[EXEC_LOAD_EVENT - 1] = Self::EXEC_LOAD_HEADER_LEN;
                event.post_header_len[DELETE_FILE_EVENT - 1] = Self::DELETE_FILE_HEADER_LEN as u8;
                event.post_header_len[NEW_LOAD_EVENT - 1] = event.post_header_len[LOAD_EVENT - 1];
            }
            1 => {
                event.server_version = Option::Some(String::from("3.23"));
                event.common_header_len = Self::OLD_HEADER_LEN as usize;
                event.number_of_event_types = FORMAT_DESCRIPTION_EVENT - 1;
                event.post_header_len[START_EVENT_V3 - 1] = Self::START_V3_HEADER_LEN as u8;
                event.post_header_len[QUERY_EVENT - 1] = QUERY_HEADER_MINIMAL_LEN as u8;
                event.post_header_len[LOAD_EVENT - 1] = Self::LOAD_HEADER_LEN as u8;
                event.post_header_len[CREATE_FILE_EVENT - 1] = Self::CREATE_FILE_HEADER_LEN;
                event.post_header_len[APPEND_BLOCK_EVENT - 1] = Self::APPEND_BLOCK_HEADER_LEN;
                event.post_header_len[EXEC_LOAD_EVENT - 1] = Self::EXEC_LOAD_HEADER_LEN;
                event.post_header_len[DELETE_FILE_EVENT - 1] = Self::DELETE_FILE_HEADER_LEN as u8;
                event.post_header_len[NEW_LOAD_EVENT - 1] = event.post_header_len[LOAD_EVENT - 1];
            }
            _ => {
                event.number_of_event_types = 0;
                event.common_header_len = 0;
            }
        }
        event
    }


    pub fn binlog_version(&self) -> u16 {
        self.binlog_version
    }
    pub fn server_version(&self) -> &str {
        self.server_version.as_ref().unwrap()
    }

    pub fn calc_server_version_split(&mut self) {
        Self::do_server_version_split(&self.server_version.as_ref().unwrap(), &mut self.server_version_split);
    }

    pub fn do_server_version_split(server_version: &str, server_version_split: &mut [u8; 3]) {
        let split: Vec<&str> = server_version.split(".").collect();
        if split.len() < 3 {
            server_version_split[0] = 0;
            server_version_split[1] = 0;
            server_version_split[2] = 0;
        } else {
            let mut j = 0;
            let mut i = 0;
            while i < 3 {
                let str = split[i];
                for char in str.chars() {
                    if char.is_ascii_digit() {
                        break;
                    }
                }
                if j > 0 {
                    server_version_split[i] = str[0..j].parse::<u8>().unwrap();
                } else {
                    server_version_split[0] = 0;
                    server_version_split[1] = 0;
                    server_version_split[2] = 0;
                }
                i += 1;
            }
        }
    }

    pub fn get_version_product(&self) -> u32 {
        FormatDescriptionLogEvent::version_product(&self.server_version_split)
    }
    pub fn version_product(server_version_split: &[u8; 3]) -> u32 {
        (server_version_split[0] as u32 * 256 + server_version_split[1] as u32) * 256 + server_version_split[2] as u32
    }
    pub fn new() -> Self {
        Self {
            start_log_event_v3: StartLogEventV3::new(),
            binlog_version: 0,
            server_version: Option::None,
            common_header_len: 0,
            number_of_event_types: 0,
            post_header_len: Box::from(vec![]),
            server_version_split: [0, 0, 0],
        }
    }
}

pub struct GtidLogEvent {
    event: Event,
    commit_flag: bool,
    sid: Uuid,
    gno: i64,
    last_committed: Option<i64>,
    sequence_number: Option<i64>,
}

impl GtidLogEvent {
    const ENCODED_FLAG_LENGTH: u8 = 1;
    const ENCODED_SID_LENGTH: u8 = 16;
    const LOGICAL_TIMESTAMP_TYPE_CODE: u8 = 2;

    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Self {
        let mut event = GtidLogEvent {
            event: Event::new(),
            commit_flag: false,
            sid: Default::default(),
            gno: 0,
            last_committed: None,
            sequence_number: None,
        };
        event.event.header = header.clone();

        let common_header_len = description_event.common_header_len;
        buffer.up_position(common_header_len);
        event.commit_flag = (buffer.get_uint8().unwrap() != 0);

        let bs = buffer.get_data_len(Self::ENCODED_SID_LENGTH as usize);

        let high = get_i64(&bs[0..8]) as u64;
        let low = get_i64(&bs[8..16]) as u64;
        event.sid = Uuid::from_u64_pair(high, low);
        event.gno = buffer.get_int64().unwrap();

        if buffer.has_remaining() && buffer.remaining() > 16 && buffer.get_uint8().unwrap() == Self::LOGICAL_TIMESTAMP_TYPE_CODE {
            event.last_committed = Option::Some(buffer.get_int64().unwrap());
            event.sequence_number = Option::Some(buffer.get_int64().unwrap());
        }
        event
    }


    pub fn commit_flag(&self) -> bool {
        self.commit_flag
    }
    pub fn sid(&self) -> Uuid {
        self.sid
    }
    pub fn gno(&self) -> i64 {
        self.gno
    }
    pub fn last_committed(&self) -> Option<i64> {
        self.last_committed
    }
    pub fn sequence_number(&self) -> Option<i64> {
        self.sequence_number
    }

    pub fn get_gtid(&self) -> String {
        let mut gtid = String::new();
        gtid.push_str(self.sid.to_string().as_str());
        gtid.push_str(self.gno.to_string().as_str());
        gtid
    }
}

pub struct HeartbeatLogEvent {
    event: Event,
    ident_len: usize,
    log_ident: String,
}


impl HeartbeatLogEvent {
    const FN_REF_LEN: usize = 512;

    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Option<HeartbeatLogEvent> {
        let mut event = HeartbeatLogEvent {
            event: Event::new(),
            ident_len: 0,
            log_ident: "".to_string(),
        };

        event.event.header = header.clone();
        let common_header_len = description_event.common_header_len;
        event.ident_len = buffer.limit() - common_header_len;
        if event.ident_len > Self::FN_REF_LEN - 1 {
            event.ident_len = Self::FN_REF_LEN
        }
        event.log_ident = buffer.get_full_string_pos_len(common_header_len, event.ident_len)?;

        Option::Some(event)
    }
}

pub struct IgnorableLogEvent {
    event: Event,
}

impl IgnorableLogEvent {
    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> IgnorableLogEvent {
        let mut event = IgnorableLogEvent {
            event: Event::new()
        };
        event.event.header = header.clone();
        event
    }
}

pub struct IncidentLogEvent {
    event: Event,
    incident: usize,
    message: Option<String>,
}

impl IncidentLogEvent {
    const INCIDENT_NONE: usize = 0;
    const INCIDENT_LOST_EVENTS: i32 = 1;
    const INCIDENT_COUNT: usize = 1;

    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Option<IncidentLogEvent> {
        let mut event = IncidentLogEvent {
            event: Event::new(),
            incident: 0,
            message: None,
        };
        event.event.header = header.clone();
        let common_header_len = description_event.common_header_len;
        let post_header_len = description_event.post_header_len[header.kind - 1] as usize;

        buffer.up_position(common_header_len);

        let incident_number = buffer.get_uint16().ok()? as usize;

        if incident_number >= Self::INCIDENT_COUNT || incident_number <= Self::INCIDENT_NONE {
            event.incident = Self::INCIDENT_NONE;
            event.message = None;
            return Option::Some(event);
        }
        event.incident = incident_number;
        buffer.up_position(common_header_len + post_header_len);
        event.message = buffer.get_string().ok()?;
        Option::Some(event)
    }


    pub fn message(&self) -> &Option<String> {
        &self.message
    }
}

pub struct InvarianceLogEvent {
    event: Event,
    value: i64,
    kind: i8,

}

impl InvarianceLogEvent {
    const I_TYPE_OFFSET: usize = 0;
    const I_VAL_OFFSET: usize = 1;
    const INVALID_INT_EVENT: u8 = 0;
    const LAST_INSERT_ID_EVENT: u8 = 1;
    const INSERT_ID_EVENT: u8 = 2;

    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Option<InvarianceLogEvent> {
        let mut event = InvarianceLogEvent {
            event: Event::new(),
            value: 0,
            kind: 0,
        };
        event.event.header = header.clone();

        buffer.up_position(description_event.common_header_len + description_event.post_header_len[INTVAR_EVENT - 1] as usize + Self::I_TYPE_OFFSET);
        event.kind = buffer.get_int8().ok()?;
        event.value = buffer.get_int64().ok()?;

        Option::Some(event)
    }
}

pub struct LoadLogEvent {
    event: Event,
    table: Option<String>,
    db: Option<String>,
    frame: Option<String>,
    skip_lines: u32,
    num_fields: u32,
    fields: Vec<String>,
    field_term: Option<String>,
    line_term: Option<String>,
    line_start: Option<String>,
    enclosed: Option<String>,
    escaped: Option<String>,
    empty_flags: u8,
    opt_flags: u8,
    exec_time: u32,
}

impl LoadLogEvent {
    const L_THREAD_ID_OFFSET: u8 = 0;
    const L_EXEC_TIME_OFFSET: usize = 4;
    const L_SKIP_LINES_OFFSET: u8 = 8;
    const L_TBL_LEN_OFFSET: u8 = 12;
    const L_DB_LEN_OFFSET: u8 = 13;
    const L_NUM_FIELDS_OFFSET: u8 = 14;
    const L_SQL_EX_OFFSET: u8 = 18;
    const L_DATA_OFFSET: usize = FormatDescriptionLogEvent::LOAD_HEADER_LEN;
    const DUMPFILE_FLAG: u8 = 0x1;
    const OPT_ENCLOSED_FLAG: u8 = 0x2;
    const REPLACE_FLAG: u8 = 0x4;
    const IGNORE_FLAG: u8 = 0x8;
    const FIELD_TERM_EMPTY: u8 = 0x1;
    const ENCLOSED_EMPTY: u8 = 0x2;
    const LINE_TERM_EMPTY: u8 = 0x4;
    const LINE_START_EMPTY: u8 = 0x8;
    const ESCAPED_EMPTY: u8 = 0x10;

    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Self {
        let mut event = LoadLogEvent {
            event: Event::new(),
            table: Option::None,
            db: Option::None,
            frame: Option::None,
            skip_lines: 0,
            num_fields: 0,
            fields: vec![],
            field_term: Option::None,
            line_term: Option::None,
            line_start: Option::None,
            enclosed: Option::None,
            escaped: Option::None,
            empty_flags: 0,
            opt_flags: 0,
            exec_time: 0,
        };
        event.event.from(header.clone());
        let load_header_len = FormatDescriptionLogEvent::LOAD_HEADER_LEN;
        let body_offset = if event.event.header.kind == LOAD_EVENT { load_header_len + description_event.common_header_len } else { load_header_len + FormatDescriptionLogEvent::LOG_EVENT_HEADER_LEN };
        event.copy_log_event(buffer, body_offset, description_event);
        event
    }

    pub fn copy_log_event(&mut self, buffer: &mut LogBuffer, body_offset: usize, description_event: &FormatDescriptionLogEvent) {
        buffer.up_position(description_event.common_header_len + Self::L_EXEC_TIME_OFFSET);
        self.exec_time = buffer.get_uint32().unwrap();
        self.skip_lines = buffer.get_uint32().unwrap();
        let table_name_len = buffer.get_uint8().unwrap();
        let db_len = buffer.get_uint8().unwrap();
        self.num_fields = buffer.get_uint32().unwrap();
        buffer.up_position(body_offset);

        if self.event.header.kind != LOAD_EVENT {
            self.field_term = buffer.get_string().unwrap();
            self.enclosed = buffer.get_string().unwrap();
            self.line_term = buffer.get_string().unwrap();
            self.line_start = buffer.get_string().unwrap();
            self.opt_flags = buffer.get_int8().unwrap() as u8;
            self.empty_flags = 0;
        } else {
            self.field_term = buffer.get_fix_string_len(1);
            self.enclosed = buffer.get_fix_string_len(1);
            self.line_term = buffer.get_fix_string_len(1);
            self.line_start = buffer.get_fix_string_len(1);
            self.escaped = buffer.get_fix_string_len(1);
            self.opt_flags = buffer.get_uint8().unwrap();
            self.empty_flags = buffer.get_uint8().unwrap();

            if self.empty_flags & LoadLogEvent::FIELD_TERM_EMPTY != 0 { self.field_term = Option::None }
            if self.empty_flags & LoadLogEvent::ENCLOSED_EMPTY != 0 { self.enclosed = Option::None }
            if self.empty_flags & LoadLogEvent::LINE_TERM_EMPTY != 0 { self.line_term = Option::None }
            if self.empty_flags & LoadLogEvent::LINE_START_EMPTY != 0 { self.line_start = Option::None }
            if self.empty_flags & LoadLogEvent::ESCAPED_EMPTY != 0 { self.escaped = Option::None }
        }

        self.table = buffer.get_fix_string_len(table_name_len as usize + 1);
        self.db = buffer.get_fix_string_len(db_len as usize + 1);
        let from = buffer.position();
        let end = buffer.limit() + from;
        let mut found = from;
        while (found < end) && buffer.get_int8_pos(found).unwrap() as u8 as char != '\0' {
            found += 1;
        }
        self.frame = buffer.get_string_pos(found).unwrap();
        buffer.forward(1);
    }
}

// pub struct LogEventHeader {
//     table: Option<String>,
//     db: Option<String>,
//     fname: Option<String>,
//     skip_lines: i32,
//     num_fields: i32,
//     fields: Option<Vec<String>>,
//     field_term: Option<String>,
//     line_term: Option<String>,
//     line_start: Option<String>,
//     enclosed: Option<String>,
//     escaped: Option<String>,
//     opt_flags: Option<String>,
//     empty_flags: Option<String>,
//     exec_time: Option<String>,
// }
//
//
// impl LogEventHeader {
//     const L_THREAD_ID_OFFSET :usize = 0;
//     const L_EXEC_TIME_OFFSET :usize = 4;
//     const L_SKIP_LINES_OFFSET:usize = 8;
//     const L_TBL_LEN_OFFSET   :usize = 12;
//     const L_DB_LEN_OFFSET    :usize = 13;
//     const L_NUM_FIELDS_OFFSET:usize = 14;
//     const L_SQL_EX_OFFSET    :usize = 18;
//     const L_DATA_OFFSET      :usize = FormatDescriptionLogEvent::LOAD_HEADER_LEN;
//
//
//     const DUMPFILE_FLAG      :u8 = 0x1;
//     const OPT_ENCLOSED_FLAG  :u8 = 0x2;
//     const REPLACE_FLAG       :u8 = 0x4;
//     const IGNORE_FLAG        :u8 = 0x8;
//     const FIELD_TERM_EMPTY   :u8 = 0x1;
//     const ENCLOSED_EMPTY     :u8 = 0x2;
//     const LINE_TERM_EMPTY    :u8 = 0x4;
//     const LINE_START_EMPTY   :u8 = 0x8;
//     const ESCAPED_EMPTY      :u8 = 0x10;
//
//
//
// }

pub struct PreviousGtidsLogEvent {
    event: Event,
}

impl PreviousGtidsLogEvent {
    pub fn from(header: &LogHeader, _buffer: &mut LogBuffer, _description_event: &FormatDescriptionLogEvent) -> PreviousGtidsLogEvent {
        let mut event = PreviousGtidsLogEvent {
            event: Event::new(),
        };
        event.event.header = header.clone();
        event
    }
}


pub struct QueryLogEvent {
    event: Event,
    user: Option<String>,
    host: Option<String>,
    query: Option<String>,
    catalog: Option<String>,
    dbname: Option<String>,
    exec_time: u32,
    error_code: u16,
    session_id: u32,
    flags2: u32,
    sql_mode: i64,
    auto_increment_increment: u16,
    auto_increment_offset: u16,
    client_charset: u16,
    client_collation: u16,
    server_collation: u16,
    tv_sec: i32,
    ddl_xid: u64,
    charset_name: Option<String>,
    time_zone: Option<String>,
}

impl QueryLogEvent {
    /**
     * The maximum number of updated databases that a status of Query-log-event
     * can carry. It can redefined within a range [1..
     * OVER_MAX_DBS_IN_EVENT_MTS].
     */
    const MAX_DBS_IN_EVENT_MTS: u8 = 16;

    /**
     * When the actual number of databases exceeds MAX_DBS_IN_EVENT_MTS the
     * value of OVER_MAX_DBS_IN_EVENT_MTS is is put into the mts_accessed_dbs
     * status.
     */
    const OVER_MAX_DBS_IN_EVENT_MTS: u8 = 254;

    const SYSTEM_CHARSET_MBMAXLEN: u8 = 3;
    const NAME_CHAR_LEN: u8 = 64;
    /* Field/table name length */
    const NAME_LEN: u32 = (QueryLogEvent::NAME_CHAR_LEN * QueryLogEvent::SYSTEM_CHARSET_MBMAXLEN) as u32;

    /**
     * Max number of possible extra bytes in a replication event compared to a
     * packet (i.e. a query) sent from client to master; First, an auxiliary
     * log_event status vars estimation:
     */
    const MAX_SIZE_LOG_EVENT_STATUS: u32 = ((1 + 4 /* type, flags2 */
        + 1 + 8 /*
                                                                  * type,
                                                                  * sql_mode
                                                                  */
        + 1 + 1 + 255/*
                                                                       * type,
                                                                       * length
                                                                       * ,
                                                                       * catalog
                                                                       */
        + 1 + 4 /*
                                                                  * type,
                                                                  * auto_increment
                                                                  */
        + 1 + 6 /*
                                                                  * type,
                                                                  * charset
                                                                  */
        + 1 + 1 + 255 /*
                                                                        * type,
                                                                        * length
                                                                        * ,
                                                                        * time_zone
                                                                        */
        + 1 + 2 /*
                                                                  * type,
                                                                  * lc_time_names_number
                                                                  */
        + 1 + 2 /*
                                                                  * type,
                                                                  * charset_database_number
                                                                  */
        + 1 + 8 /*
                                                                  * type,
                                                                  * table_map_for_update
                                                                  */
        + 1 + 4 /*
                                                                  * type,
                                                                  * master_data_written
                                                                  */
        /*
         * type, db_1, db_2,
         * ...
         */
        /* type, microseconds */
        /*
         * MariaDb type,
         * sec_part of NOW()
         */
        + 1 + (QueryLogEvent::MAX_DBS_IN_EVENT_MTS as u32 * (1 + QueryLogEvent::NAME_LEN as u32)) + 3 /*
                                                                                                            * type
                                                                                                            * ,
                                                                                                            * microseconds
                                                                                                            */ + 1 + 32
        * 3 + 1 + 60/*
                                                                      * type ,
                                                                      * user_len
                                                                      * , user ,
                                                                      * host_len
                                                                      * , host
                                                                      */)
        + 1 + 1 /*
                                                                 * type,
                                                                 * explicit_def
                                                                 * ..ts
                                                                 */ + 1 + 8 /*
                                                                            * type,
                                                                            * xid
                                                                            * of
                                                                            * DDL
                                                                            */ + 1 + 2 /*
                                                                                       * type
                                                                                       * ,
                                                                                       * default_collation_for_utf8mb4_number
                                                                                       */ + 1) as u32 /* sql_require_primary_key */;

    const Q_THREAD_ID_OFFSET: u8 = 0;
    const Q_EXEC_TIME_OFFSET: u8 = 4;
    const Q_DB_LEN_OFFSET: u8 = 8;
    const Q_ERR_CODE_OFFSET: u8 = 9;
    const Q_STATUS_VARS_LEN_OFFSET: u8 = 11;
    const Q_DATA_OFFSET: u8 = QUERY_HEADER_LEN;
    const Q_FLAGS2_CODE: u8 = 0;
    const Q_SQL_MODE_CODE: u8 = 1;
    const Q_CATALOG_CODE: u8 = 2;
    const Q_AUTO_INCREMENT: u8 = 3;
    const Q_CHARSET_CODE: u8 = 4;
    const Q_TIME_ZONE_CODE: u8 = 5;
    const Q_CATALOG_NZ_CODE: u8 = 6;
    const Q_LC_TIME_NAMES_CODE: u8 = 7;
    const Q_CHARSET_DATABASE_CODE: u8 = 8;
    const Q_TABLE_MAP_FOR_UPDATE_CODE: u8 = 9;
    const Q_MASTER_DATA_WRITTEN_CODE: u8 = 10;
    const Q_INVOKER: u8 = 11;
    const Q_UPDATED_DB_NAMES: u8 = 12;
    const Q_MICROSECONDS: u8 = 13;
    const Q_COMMIT_TS: u8 = 14;
    const Q_COMMIT_TS2: u8 = 15;
    const Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP: u8 = 16;
    const Q_DDL_LOGGED_WITH_XID: u8 = 17;
    const Q_DEFAULT_COLLATION_FOR_UTF8MB4: u8 = 18;
    const Q_SQL_REQUIRE_PRIMARY_KEY: u8 = 19;
    const Q_HRNOW: u8 = 128;


    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Result<Self, String> {
        let mut event = QueryLogEvent {
            event: Event::new(),
            user: None,
            host: None,
            query: None,
            catalog: None,
            dbname: None,
            exec_time: 0,
            error_code: 0,
            session_id: 0,
            flags2: 0,
            sql_mode: 0,
            auto_increment_increment: 0,
            auto_increment_offset: 0,
            client_charset: 0,
            client_collation: 0,
            server_collation: 0,
            tv_sec: 0,
            ddl_xid: Default::default(),
            charset_name: None,
            time_zone: None,
        };
        event.event.header = header.clone();

        let common_header_len = description_event.common_header_len;
        let post_header_len = description_event.post_header_len[header.kind - 1] as usize;
        /*
         * We test if the event's length is sensible, and if so we compute
         * data_len. We cannot rely on QUERY_HEADER_LEN here as it would not be
         * format-tolerant. We use QUERY_HEADER_MINIMAL_LEN which is the same
         * for 3.23, 4.0 & 5.0.
         */
        if buffer.limit() < (common_header_len + post_header_len) {
            return Result::Err(String::from("Query event length is too short."));
        }

        let mut data_len = buffer.limit() - (common_header_len + post_header_len);
        buffer.up_position(common_header_len + QueryLogEvent::Q_AUTO_INCREMENT as usize);
        event.session_id = buffer.get_uint32().unwrap(); // Q_THREAD_ID_OFFSET
        event.exec_time = buffer.get_uint32().unwrap();  // Q_EXEC_TIME_OFFSET
        let db_len = buffer.get_uint8().unwrap() as usize;
        event.error_code = buffer.get_uint16().unwrap();
        /*
         * 5.0 format starts here. Depending on the format, we may or not have
         * affected/warnings etc The remaining post-header to be parsed has
         * length:
         */
        let mut status_vars_len = 0;
        if post_header_len > QUERY_HEADER_MINIMAL_LEN as usize {
            status_vars_len = buffer.get_uint16().unwrap() as usize;
            /*
             * Check if status variable length is corrupt and will lead to very
             * wrong data. We could be even more strict and require data_len to
             * be even bigger, but this will suffice to catch most corruption
             * errors that can lead to a crash.
             */
            if status_vars_len as usize > data_len.min(QueryLogEvent::MAX_SIZE_LOG_EVENT_STATUS as usize) {
                return Result::Err(format!("status_vars_len {} > data_len {}", status_vars_len, data_len));
            }
            data_len -= status_vars_len as usize;
        }

        /*
         * We have parsed everything we know in the post header for QUERY_EVENT,
         * the rest of post header is either comes from older version MySQL or
         * dedicated to derived events (e.g. Execute_load_query...)
         */
        let start = common_header_len + post_header_len;
        let limit = buffer.limit();
        let end = start + status_vars_len;
        buffer.up_position(start);
        buffer.new_limit(end);
        QueryLogEvent::unpack_variables(&mut event, buffer, end);
        buffer.up_position(end);
        buffer.new_limit(limit);

        let query_len = data_len - data_len - db_len - 1;
        event.dbname = buffer.get_fix_string_len(db_len + 1);
        if event.client_charset >= 0 {
            // TODO CharsetConversion
        } else {
            buffer.get_fix_string_len(query_len);
        }
        Result::Ok(event)
    }

    fn unpack_variables(event: &mut QueryLogEvent, buffer: &mut LogBuffer, end: usize) {
        let mut code = u8::MAX;
        while buffer.position() < end {
            code = buffer.get_uint8().unwrap();

            match code {
                QueryLogEvent::Q_FLAGS2_CODE =>
                    event.flags2 = buffer.get_uint32().unwrap(),
                QueryLogEvent::Q_SQL_MODE_CODE =>
                    event.sql_mode = buffer.get_int64().unwrap(),
                QueryLogEvent::Q_CATALOG_NZ_CODE =>
                    event.catalog = buffer.get_string().unwrap(),
                QueryLogEvent::Q_AUTO_INCREMENT => {
                    event.auto_increment_increment = buffer.get_uint16().unwrap();
                    event.auto_increment_offset = buffer.get_uint16().unwrap();
                }
                QueryLogEvent::Q_CHARSET_CODE => {
                    // Charset: 6 byte character set flag.
                    // 1-2 = character set client
                    // 3-4 = collation client
                    // 5-6 = collation server
                    event.client_charset = buffer.get_uint16().unwrap();
                    event.client_collation = buffer.get_uint16().unwrap();
                    event.server_collation = buffer.get_uint16().unwrap();
                }
                QueryLogEvent::Q_TIME_ZONE_CODE =>
                    event.time_zone = buffer.get_string().unwrap(),
                QueryLogEvent::Q_CATALOG_CODE => {
                    let len = buffer.get_uint8().unwrap() as usize;
                    event.catalog = buffer.get_fix_string_len(len + 1);
                }
                QueryLogEvent::Q_LC_TIME_NAMES_CODE => {
                    buffer.forward(2).unwrap();
                }
                QueryLogEvent::Q_CHARSET_DATABASE_CODE => {
                    buffer.forward(2).unwrap();
                }
                QueryLogEvent::Q_TABLE_MAP_FOR_UPDATE_CODE => {
                    buffer.forward(8).unwrap();
                }
                QueryLogEvent::Q_MASTER_DATA_WRITTEN_CODE => {
                    buffer.forward(4).unwrap();
                }
                QueryLogEvent::Q_INVOKER => {
                    event.user = buffer.get_string().unwrap();
                    event.host = buffer.get_string().unwrap();
                }
                QueryLogEvent::Q_MICROSECONDS =>
                    event.tv_sec = buffer.get_int24().unwrap(),
                QueryLogEvent::Q_UPDATED_DB_NAMES => {
                    let mut mts_accessed_dbs = buffer.get_uint8().unwrap();
                    if mts_accessed_dbs > QueryLogEvent::MAX_DBS_IN_EVENT_MTS {
                        mts_accessed_dbs = QueryLogEvent::OVER_MAX_DBS_IN_EVENT_MTS;
                        break;
                    }
                    let mut mts_accessed_db_names = vec![];
                    let mut i = 0;
                    while i < mts_accessed_dbs && buffer.position() < end {
                        let length = end - buffer.position();
                        let len_x = if length < QueryLogEvent::NAME_LEN as usize { length } else { QueryLogEvent::NAME_LEN as usize };
                        mts_accessed_db_names.push(buffer.get_fix_string_len(len_x));
                        i += 1;
                    }
                }
                QueryLogEvent::Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP => {
                    buffer.forward(1);
                }

                QueryLogEvent::Q_DDL_LOGGED_WITH_XID =>
                    event.ddl_xid = buffer.get_uint64().unwrap(),
                QueryLogEvent::Q_DEFAULT_COLLATION_FOR_UTF8MB4 =>
                    {
                        buffer.forward(2);
                    }
                QueryLogEvent::Q_SQL_REQUIRE_PRIMARY_KEY =>
                    {
                        buffer.forward(1);
                    }
                QueryLogEvent::Q_HRNOW =>
                    {
                        buffer.forward(3);
                    }
                _ =>
                    println!("Query_log_event has unknown status vars (first has code: {}), skipping the rest of them", code)
            }
        }
    }


    pub fn event(&self) -> &Event {
        &self.event
    }
    pub fn user(&self) -> &Option<String> {
        &self.user
    }
    pub fn host(&self) -> &Option<String> {
        &self.host
    }
    pub fn query(&self) -> &Option<String> {
        &self.query
    }
    pub fn catalog(&self) -> &Option<String> {
        &self.catalog
    }
    pub fn dbname(&self) -> &Option<String> {
        &self.dbname
    }
    pub fn exec_time(&self) -> u32 {
        self.exec_time
    }
    pub fn error_code(&self) -> u16 {
        self.error_code
    }
    pub fn session_id(&self) -> u32 {
        self.session_id
    }
    pub fn flags2(&self) -> u32 {
        self.flags2
    }
    pub fn sql_mode(&self) -> i64 {
        self.sql_mode
    }
    pub fn auto_increment_increment(&self) -> u16 {
        self.auto_increment_increment
    }
    pub fn auto_increment_offset(&self) -> u16 {
        self.auto_increment_offset
    }
    pub fn client_charset(&self) -> u16 {
        self.client_charset
    }
    pub fn client_collation(&self) -> u16 {
        self.client_collation
    }
    pub fn server_collation(&self) -> u16 {
        self.server_collation
    }
    pub fn tv_sec(&self) -> i32 {
        self.tv_sec
    }
    pub fn ddl_xid(&self) -> u64 {
        self.ddl_xid
    }
    pub fn charset_name(&self) -> &Option<String> {
        &self.charset_name
    }
    pub fn time_zone(&self) -> &Option<String> {
        &self.time_zone
    }

    pub fn new() -> Self {
        Self {
            event: Event::new(),
            user: Option::None,
            host: Option::None,
            query: Option::None,
            catalog: Option::None,
            dbname: Option::None,
            exec_time: 0,
            error_code: 0,
            session_id: 0,
            flags2: 0,
            sql_mode: 0,
            auto_increment_increment: 0,
            auto_increment_offset: 0,
            client_charset: 0,
            client_collation: 0,
            server_collation: 0,
            tv_sec: 0,
            ddl_xid: 0,
            charset_name: Option::None,
            time_zone: Option::None,
        }
    }
}

pub struct RandLogEvent {
    event: Event,
    seed1: i64,
    seed2: i64,
}

impl RandLogEvent {
    const RAND_SEED1_OFFSET: u8 = 0;
    const RAND_SEED2_OFFSET: u8 = 8;
    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Option<Self> {
        let mut event = RandLogEvent {
            event: Event::new(),
            seed1: 0,
            seed2: 0,
        };
        event.event.header = header.clone();
        buffer.up_position(description_event.common_header_len + description_event.post_header_len[RAND_EVENT - 1] as usize);
        event.seed1 = buffer.get_int64().ok()?;
        event.seed2 = buffer.get_int64().ok()?;
        Option::Some(event)
    }

    pub fn get_query(&self) -> String {
        String::from(format!("SET SESSION rand_seed1 = {} , rand_seed2 = {}", self.seed1, self.seed2))
    }
}

pub struct RotateLogEvent {
    event: Event,
    file_name: Option<String>,
    position: i64,
}

impl RotateLogEvent {
    const R_POS_OFFSET: usize = 0;
    const R_IDENT_OFFSET: u8 = 8;
    const FN_REFLEN: usize = 512;
    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Option<Self> {
        let mut event = RotateLogEvent {
            event: Event::new(),
            file_name: None,
            position: 0,
        };
        let common_header_len = description_event.common_header_len;
        let post_header_len = description_event.post_header_len[ROTATE_EVENT - 1] as usize;

        buffer.up_position(common_header_len + Self::R_POS_OFFSET);

        event.position = if post_header_len != 0 { buffer.get_int64().ok()? } else { 4 };

        let filename_offset = common_header_len + post_header_len;
        let mut filename_len = buffer.limit() - filename_offset;

        if filename_len > Self::FN_REFLEN - 1 {
            filename_len = Self::FN_REFLEN - 1;
        }

        buffer.up_position(filename_offset);
        event.file_name = buffer.get_fix_string_len(filename_len);
        Option::Some(event)
    }


    pub fn file_name(&self) -> &Option<String> {
        &self.file_name
    }
    pub fn position(&self) -> i64 {
        self.position
    }
}

pub struct RowsLogBuffer {}

pub struct RowsLogEvent {
    event: Event,
    table_id: u64,
    table_map_log_event: TableMapLogEvent,
    column_len: usize,
    partial: bool,
    columns: BitSet,
    change_columns: BitSet,
    json_column_count: i32,
    rows_buf: LogBuffer,
    flags: u16,
}

impl RowsLogEvent {
    const STMT_END_F: u8 = 1;
    const NO_FOREIGN_KEY_CHECKS_F: u8 = (1 << 1);
    const RELAXED_UNIQUE_CHECKS_F: u8 = (1 << 2);
    const COMPLETE_ROWS_F: u8 = (1 << 3);
    const RW_MAPID_OFFSET: u8 = 0;
    const RW_FLAGS_OFFSET: u8 = 6;
    const RW_VHLEN_OFFSET: u8 = 8;
    const RW_V_TAG_LEN: u8 = 1;
    const RW_V_EXTRAINFO_TAG: u8 = 0;

    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent, partial: bool) -> Self {
        let mut event = RowsLogEvent {
            event: Event::new(),
            table_id: 0,
            table_map_log_event: TableMapLogEvent::new(),
            column_len: 0,
            partial,
            columns: Default::default(),
            change_columns: Default::default(),
            json_column_count: 0,
            rows_buf: LogBuffer::new(),
            flags: 0,
        };
        event.event.header = header.clone();
        let common_header_len = description_event.common_header_len;
        let post_header_len = description_event.post_header_len[header.kind - 1];
        let mut header_len = 0;
        buffer.up_position(common_header_len + RowsLogEvent::RW_MAPID_OFFSET as usize);

        if post_header_len == 6 {
            /*
             * Master is of an intermediate source tree before 5.1.4. Id is 4
             * bytes
             */
            event.table_id = buffer.get_uint32().unwrap() as u64;
        } else {
            event.table_id = buffer.get_uint48().unwrap();
        }

        event.flags = buffer.get_uint16().unwrap();

        if post_header_len == FormatDescriptionLogEvent::ROWS_HEADER_LEN_V2 {
            header_len = buffer.get_uint16().unwrap();
            header_len -= 2;
            let start = buffer.position();
            let end = start + header_len as usize;
            let mut i = start;
            while i < end {
                let result = buffer.get_uint8_pos(i).unwrap();
                i += 1;
                match result {
                    RowsLogEvent::RW_V_EXTRAINFO_TAG => {
                        buffer.up_position(i + EXTRA_ROW_INFO_LEN_OFFSET as usize);
                        let check_len = buffer.get_uint8().unwrap();
                        let val = check_len - EXTRA_ROW_INFO_HDR_BYTES;// EXTRA_ROW_INFO_LEN_OFFSET
                        assert_eq!(buffer.get_uint8().unwrap(), val);
                        let mut j = 0;
                        while j < val {
                            assert_eq!(buffer.get_uint8().unwrap(), val);
                            j += 1;
                        }
                    }
                    _ => i = end
                }
            }
        }

        buffer.up_position(common_header_len + post_header_len as usize + header_len as usize);
        event.column_len = buffer.get_packed_i64() as usize;
        event.partial = partial;
        event.columns = buffer.get_bit_map(event.column_len);
        if header.kind == UPDATE_ROWS_EVENT_V1 || header.kind == UPDATE_ROWS_EVENT
            || header.kind == PARTIAL_UPDATE_ROWS_EVENT {
            event.change_columns = buffer.get_bit_map(event.column_len);
        } else {
            event.change_columns = event.columns.clone();
        }
        let data_size = buffer.limit() - buffer.position();
        event.rows_buf = buffer.duplicate_len(data_size).unwrap();
        event
    }

    pub fn fill_table(&mut self, context: &mut LogContext) {
        self.table_map_log_event = context.get_table(self.table_id).clone();

        if self.flags & Self::STMT_END_F as u16 != 0 {
            context.clear_all_tables()
        }

        let mut json_column_count = 0;
        let column_cnt = self.table_map_log_event.column_cnt() as usize;

        let column_info = self.table_map_log_event.column_info();

        let mut i = 0;
        while i < column_cnt {
            let info = column_info.get(i).unwrap();

            if info.kind == MYSQL_TYPE_JSON {
                json_column_count += 1;
            }
            self.json_column_count = json_column_count;
            i += 1;
        }
    }
    pub fn new() -> Self {
        Self {
            event: Event::new(),
            table_id: 0,
            table_map_log_event: TableMapLogEvent::new(),
            column_len: 0,
            partial: false,
            columns: BitSet::default(),
            change_columns: BitSet::default(),
            json_column_count: 0,
            rows_buf: LogBuffer::new(),
            flags: 0,
        }
    }
}

pub struct RowsQueryLogEvent {
    ignorable_log_event: IgnorableLogEvent,
    rows_query: Option<String>,
}

impl RowsQueryLogEvent {
    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Option<Self> {
        let ignorable_log_event = IgnorableLogEvent::from(header, buffer, description_event);

        let common_header_len = description_event.common_header_len;
        let post_header_len = description_event.post_header_len[header.kind - 1] as usize;

        let offset = common_header_len + post_header_len + 1;

        let len = buffer.limit() - offset;

        let mut event = RowsQueryLogEvent {
            ignorable_log_event,
            rows_query: None,
        };
        event.rows_query = Option::Some(buffer.get_full_string_pos_len(offset, len).unwrap());
        Option::Some(event)
    }
}

pub struct StartLogEventV3 {
    event: Event,
    binlog_version: u16,
    server_version: Option<String>,
}

impl StartLogEventV3 {
    const ST_SERVER_VER_LEN: usize = 50;
    const ST_BINLOG_VER_OFFSET: usize = 0;
    const ST_SERVER_VER_OFFSET: usize = 2;


    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Option<Self> {
        let mut event = StartLogEventV3 {
            event: Event::new(),
            binlog_version: 0,
            server_version: None,
        };
        event.event.header = header.clone();
        buffer.up_position(description_event.common_header_len);
        event.binlog_version = buffer.get_uint16().ok()?;
        event.server_version = Option::Some(buffer.get_fix_string_len(Self::ST_SERVER_VER_LEN)?);
        Option::Some(event)
    }

    pub fn from_none() -> Self {
        let mut event = StartLogEventV3 {
            event: Event::new(),
            binlog_version: 0,
            server_version: None,
        };
        event.event.header = LogHeader::from_kind(START_EVENT_V3);
        event
    }


    pub fn binlog_version(&self) -> u16 {
        self.binlog_version
    }
    pub fn server_version(&self) -> &Option<String> {
        &self.server_version
    }
    pub fn new() -> Self {
        Self { event: Event::new(), binlog_version: 0, server_version: None }
    }
}

pub struct StopLogEvent {
    event: Event,
}

impl StopLogEvent {
    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Option<Self> {
        let mut event = StopLogEvent {
            event: Event::new()
        };
        event.event.header = header.clone();
        Option::Some(event)
    }
}

pub struct TableMapLogEvent {
    event: Event,
    dbname: Option<String>,
    tblname: Option<String>,
    column_cnt: i64,
    column_info: Vec<ColumnInfo>,
    table_id: u64,
    null_bits: bit_set::BitSet,
    default_charset: i32,
    exist_optional_meta_data: bool,
}

impl TableMapLogEvent {
    const TM_MAPID_OFFSET: usize = 0;
    const TM_FLAGS_OFFSET: u8 = 6;
    const SIGNEDNESS: u8 = 1;
    const DEFAULT_CHARSET: u8 = 2;
    const COLUMN_CHARSET: u8 = 3;
    const COLUMN_NAME: u8 = 4;
    const SET_STR_VALUE: u8 = 5;
    const ENUM_STR_VALUE: u8 = 6;
    const GEOMETRY_TYPE: u8 = 7;
    const SIMPLE_PRIMARY_KEY: u8 = 8;
    const PRIMARY_KEY_WITH_PREFIX: u8 = 9;
    const ENUM_AND_SET_DEFAULT_CHARSET: u8 = 10;
    const ENUM_AND_SET_COLUMN_CHARSET: u8 = 11;
    const COLUMN_VISIBILITY: u8 = 12;

    pub fn from(header: LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Result<TableMapLogEvent, String> {
        let mut event = TableMapLogEvent {
            event: Event::new(),
            dbname: Option::None,
            tblname: Option::None,
            column_cnt: 0,
            column_info: vec![],
            table_id: 0,
            null_bits: Default::default(),
            default_charset: 0,
            exist_optional_meta_data: false,
        };
        let common_header_len = description_event.common_header_len;
        let post_header_len = description_event.post_header_len[event.event.header.kind - 1] as usize;
        buffer.up_position(common_header_len + TableMapLogEvent::TM_MAPID_OFFSET);


        if post_header_len == 6 {
            event.table_id = buffer.get_uint32().unwrap() as u64;
        } else {
            event.table_id = buffer.get_uint48().unwrap();
        }


        buffer.up_position(common_header_len + post_header_len);
        event.dbname = buffer.get_string().unwrap();
        buffer.forward(1);
        event.tblname = buffer.get_string().unwrap();
        buffer.forward(1);
        event.column_cnt = buffer.get_packed_i64();
        let mut i = 0;
        while i < event.column_cnt {
            let mut info = ColumnInfo::new();
            info.kind = buffer.get_uint8().unwrap();
            event.column_info.push(info);
            i += 1;
        }

        if buffer.position() < buffer.limit() {
            let field_size = buffer.get_packed_i64();

            TableMapLogEvent::decode_fields(&mut event, buffer, field_size as usize);
            event.null_bits = buffer.get_bit_map(event.column_cnt as usize);

            let mut i = 0 as usize;


            while i < event.column_cnt as usize {
                if (event.null_bits).contains(i as usize) {
                    event.column_info[i].nullable = true;
                }
                i += 1;
            }
        }

        event.exist_optional_meta_data = false;
        let mut default_charset_pairs: Option<Vec<Pair>> = Option::None;
        let mut column_charsets: Option<Vec<i32>> = Option::None;
        while buffer.has_remaining() {
            let kind = buffer.get_uint8().unwrap();
            let len = buffer.get_packed_i64() as usize;
            match kind {
                TableMapLogEvent::SIGNEDNESS => TableMapLogEvent::parse_signedness(&mut event, buffer, len),
                TableMapLogEvent::DEFAULT_CHARSET => {
                    default_charset_pairs = TableMapLogEvent::parse_default_charset(&mut event, buffer, len);
                }
                TableMapLogEvent::COLUMN_CHARSET => {
                    column_charsets = TableMapLogEvent::parse_column_charset(buffer, len)
                }
                TableMapLogEvent::COLUMN_NAME => {
                    // set @@global.binlog_row_metadata='FULL'
                    // 主要是补充列名相关信息
                    event.exist_optional_meta_data = true;
                    TableMapLogEvent::parse_column_name(&mut event, buffer, len);
                }
                TableMapLogEvent::SET_STR_VALUE => TableMapLogEvent::parse_set_str_value(&mut event, buffer, len, true),
                TableMapLogEvent::ENUM_STR_VALUE => TableMapLogEvent::parse_set_str_value(&mut event, buffer, len, false),
                TableMapLogEvent::GEOMETRY_TYPE => TableMapLogEvent::parse_geometry_type(&mut event, buffer, len),
                TableMapLogEvent::SIMPLE_PRIMARY_KEY => TableMapLogEvent::parse_simple_pk(&mut event, buffer, len),
                TableMapLogEvent::PRIMARY_KEY_WITH_PREFIX => TableMapLogEvent::parse_pk_with_prefix(&mut event, buffer, len),
                TableMapLogEvent::ENUM_AND_SET_DEFAULT_CHARSET => {
                    TableMapLogEvent::parse_default_charset(&mut event, buffer, len);
                }
                TableMapLogEvent::ENUM_AND_SET_COLUMN_CHARSET => {
                    TableMapLogEvent::parse_column_charset(buffer, len);
                }
                TableMapLogEvent::COLUMN_VISIBILITY => TableMapLogEvent::parse_column_visibility(&mut event, buffer, len),
                _ => {
                    return Result::Err(format!("unknown kind:  {}", kind));
                }
            }

            if event.exist_optional_meta_data {
                let mut index = 0 as usize;
                let mut char_col_index = 0;
                let mut i = 0;
                while i < event.column_cnt as usize {
                    let mut cs = -1;
                    let kind = TableMapLogEvent::get_real_type(event.column_info[i].kind, event.column_info[i].meta);
                    if TableMapLogEvent::is_character_type(kind) {
                        if let Some(paris) = &default_charset_pairs {
                            if !paris.is_empty() {
                                if index < paris.len() && char_col_index == paris[i].col_index {
                                    cs = paris[i].col_charset;
                                    index += 1;
                                } else {
                                    cs = event.default_charset
                                }
                                char_col_index += 1;
                            }
                        } else if let Some(charsets) = &column_charsets {
                            cs = charsets[index];
                            index += 1;
                        }
                        (event.column_info)[i].charset = cs;
                    }
                    i += 1;
                }
            }
        }
        Result::Ok(event)
    }

    fn decode_fields(event: &mut TableMapLogEvent, buffer: &mut LogBuffer, len: usize) {
        let limit = buffer.limit();
        let new_limit = len + buffer.position();
        buffer.new_limit(new_limit);

        let mut i = 0;
        while i < event.column_cnt as usize {
            let info = &mut event.column_info[i];
            let mut binlog_type = info.kind;
            if binlog_type == MYSQL_TYPE_TYPED_ARRAY {
                binlog_type = buffer.get_uint8().unwrap();
            }

            match binlog_type {
                MYSQL_TYPE_TINY_BLOB |
                MYSQL_TYPE_BLOB |
                MYSQL_TYPE_MEDIUM_BLOB |
                MYSQL_TYPE_LONG_BLOB |
                MYSQL_TYPE_DOUBLE |
                MYSQL_TYPE_FLOAT |
                MYSQL_TYPE_GEOMETRY |
                MYSQL_TYPE_TIME2 |
                MYSQL_TYPE_DATETIME2 |
                MYSQL_TYPE_TIMESTAMP2 |
                MYSQL_TYPE_JSON => {
                    info.meta = buffer.get_uint8().unwrap() as u16;
                }
                MYSQL_TYPE_SET |
                MYSQL_TYPE_ENUM |
                MYSQL_TYPE_STRING |
                MYSQL_TYPE_NEWDECIMAL => {
                    let mut x = (buffer.get_uint8().unwrap() as u16) << 8;
                    x += (buffer.get_uint8().unwrap() as u16);
                    info.meta = x;
                }
                MYSQL_TYPE_BIT => {
                    info.meta = buffer.get_uint16().unwrap();
                }
                MYSQL_TYPE_VARCHAR => {
                    info.meta = buffer.get_uint16().unwrap();
                }
                _ => {
                    info.meta = 0;
                }
            }
            i += 1;
        }
        buffer.new_limit(limit);
    }

    fn parse_signedness(event: &mut TableMapLogEvent, buffer: &mut LogBuffer, length: usize) {
        let mut datas = vec![];
        let mut i = 0;
        while i < length {
            let ut = buffer.get_uint8().unwrap();
            let mut c = 0x80;
            while c != 0 {
                datas.push((ut & c) > 0);
                c >>= 1;
            }
            i += 1;
        }

        let mut i = 0;
        let mut index = 0;
        while i < event.column_cnt as usize {
            if TableMapLogEvent::is_numeric_type(event.column_info[i].kind) {
                event.column_info[i].unsigned = datas[index];
                index += 1;
            }
            i += 1;
        }
    }

    fn parse_default_charset(event: &mut TableMapLogEvent, buffer: &mut LogBuffer, length: usize) -> Option<Vec<Pair>> {
        let limit = buffer.position() + length;
        event.default_charset = buffer.get_packed_i64() as i32;
        let mut datas = vec![];
        while buffer.has_remaining() && buffer.position() < limit {
            let col_index = buffer.get_packed_i64() as i32;
            let col_charset = buffer.get_packed_i64() as i32;
            let mut pair = Pair::new();
            pair.col_index = col_index;
            pair.col_charset = col_charset;
            datas.push(pair)
        }
        Option::Some(datas)
    }

    fn parse_column_charset(buffer: &mut LogBuffer, length: usize) -> Option<Vec<i32>> {
        let limit = buffer.position() + length;
        let mut datas = vec![];
        while buffer.has_remaining() && buffer.position() < limit {
            let col_charset = buffer.get_packed_i64() as i32;
            datas.push(col_charset);
        }

        Option::Some(datas)
    }

    fn parse_column_visibility(event: &mut TableMapLogEvent, buffer: &mut LogBuffer, length: usize) {
        let mut datas = vec![];
        let mut i = 0;
        while i < length {
            let ut = buffer.get_uint8().unwrap();
            let mut c = 0x80;
            while c != 0 {
                datas.push((ut & c) > 0);
                c >>= 1;
            }
            i += 1;
        }

        let mut i = 0;
        while i < event.column_cnt as usize {
            event.column_info[i].visibility = datas[i];
            i += 1;
        }
    }

    fn parse_column_name(event: &mut TableMapLogEvent, buffer: &mut LogBuffer, length: usize) {
        let limit = buffer.position() + length;
        let index = 0;
        while buffer.has_remaining() && buffer.position() < limit {
            let len = buffer.get_packed_i64() as usize;
            event.column_info[index].name = buffer.get_fix_string_len(len).unwrap();
        }
    }

    fn parse_set_str_value(event: &mut TableMapLogEvent, buffer: &mut LogBuffer, length: usize, set: bool) {
        let limit = buffer.position() + length;
        let mut datas = vec![];
        while buffer.has_remaining() && buffer.position() < limit {
            let count = buffer.get_packed_i64() as i32;
            let mut data = vec![];
            let i = 0;
            while i < count {
                let len1 = buffer.get_packed_i64() as usize;
                data.push(buffer.get_fix_string_len(len1).unwrap())
            }
            datas.push(data)
        }

        let mut index = 0;
        let mut i = 0;
        while i < event.column_cnt as usize {
            if set && TableMapLogEvent::get_real_type(event.column_info[i].kind, event.column_info[i].meta) == MYSQL_TYPE_SET {
                event.column_info[i].set_enum_values = datas[index].clone();
                index += 1;
            }
            if !set && TableMapLogEvent::get_real_type(event.column_info[i].kind, event.column_info[i].meta) == MYSQL_TYPE_ENUM {
                event.column_info[i].set_enum_values = datas[index].clone();
                index += 1;
            }
            i += 1;
        }
    }

    fn parse_geometry_type(event: &mut TableMapLogEvent, buffer: &mut LogBuffer, length: usize) {
        let limit = buffer.position() + length;
        let mut datas = vec![];
        while buffer.has_remaining() && buffer.position() < limit {
            let col_type = buffer.get_packed_i64() as i32;
            datas.push(col_type);
        }
        let mut index = 0;
        let mut i = 0;

        while i < event.column_cnt as usize {
            if event.column_info[i].kind == MYSQL_TYPE_GEOMETRY {
                event.column_info[i].geo_type = datas[index];
                index += 1;
            }
            i += 1;
        }
    }

    fn parse_simple_pk(event: &mut TableMapLogEvent, buffer: &mut LogBuffer, length: usize) {
        // stores primary key's column information extracted from
        // field. Each column has an index and a prefix which are
        // stored as a unit_pair. prefix is always 0 for
        // SIMPLE_PRIMARY_KEY field.
        let limit = buffer.position() + length;
        while buffer.has_remaining() && buffer.position() < limit {
            let col_index = buffer.get_packed_i64() as usize;
            event.column_info[col_index].pk = true;
        }
    }

    fn parse_pk_with_prefix(event: &mut TableMapLogEvent, buffer: &mut LogBuffer, length: usize) {
        let limit = buffer.position() + length;
        while buffer.has_remaining() && buffer.position() < limit {
            let col_index = buffer.get_packed_i64() as usize;
            // prefix length, 比如 char(32)
            let col_prefix = buffer.get_packed_i64();
            event.column_info[col_index].pk = true;
        }
    }

    fn is_numeric_type(kind: u8) -> bool {
        match kind {
            MYSQL_TYPE_TINY |
            MYSQL_TYPE_SHORT |
            MYSQL_TYPE_INT24 |
            MYSQL_TYPE_LONG |
            MYSQL_TYPE_LONGLONG |
            MYSQL_TYPE_NEWDECIMAL |
            MYSQL_TYPE_FLOAT |
            MYSQL_TYPE_DOUBLE => true,
            _ => false
        }
    }

    fn is_character_type(kind: u8) -> bool {
        match kind {
            MYSQL_TYPE_STRING |
            MYSQL_TYPE_VAR_STRING |
            MYSQL_TYPE_VARCHAR |
            MYSQL_TYPE_BLOB => true,
            _ => false
        }
    }

    fn get_real_type(mut kind: u8, meta: u16) -> u8 {
        if kind == MYSQL_TYPE_STRING {
            if meta >= 256 {
                let byte0 = (meta >> 8) as u8;
                if byte0 & 0x30 != 0x30 {
                    kind = byte0 | 0x30;
                } else {
                    match byte0 {
                        MYSQL_TYPE_SET |
                        MYSQL_TYPE_ENUM |
                        MYSQL_TYPE_STRING => kind = byte0,
                        _ => {}
                    }
                }
            }
        }
        kind
    }


    pub fn event(&self) -> &Event {
        &self.event
    }
    pub fn dbname(&self) -> &Option<String> {
        &self.dbname
    }
    pub fn tblname(&self) -> &Option<String> {
        &self.tblname
    }
    pub fn column_cnt(&self) -> i64 {
        self.column_cnt
    }
    pub fn column_info(&self) -> &Vec<ColumnInfo> {
        &self.column_info
    }
    pub fn table_id(&self) -> u64 {
        self.table_id
    }
    pub fn null_bits(&self) -> &bit_set::BitSet {
        &self.null_bits
    }
    pub fn default_charset(&self) -> i32 {
        self.default_charset
    }
    pub fn exist_optional_meta_data(&self) -> bool {
        self.exist_optional_meta_data
    }


    pub fn set_event(&mut self, event: Event) {
        self.event = event;
    }
    pub fn set_dbname(&mut self, dbname: Option<String>) {
        self.dbname = dbname;
    }
    pub fn set_tblname(&mut self, tblname: Option<String>) {
        self.tblname = tblname;
    }
    pub fn set_column_cnt(&mut self, column_cnt: i64) {
        self.column_cnt = column_cnt;
    }
    pub fn set_column_info(&mut self, column_info: Vec<ColumnInfo>) {
        self.column_info = column_info;
    }
    pub fn set_table_id(&mut self, table_id: u64) {
        self.table_id = table_id;
    }
    pub fn set_null_bits(&mut self, null_bits: bit_set::BitSet) {
        self.null_bits = null_bits;
    }
    pub fn set_default_charset(&mut self, default_charset: i32) {
        self.default_charset = default_charset;
    }
    pub fn set_exist_optional_meta_data(&mut self, exist_optional_meta_data: bool) {
        self.exist_optional_meta_data = exist_optional_meta_data;
    }
    pub fn new() -> Self {
        Self {
            event: Event::new(),
            dbname: Option::None,
            tblname: Option::None,
            column_cnt: 0,
            column_info: vec![],
            table_id: 0,
            null_bits: BitSet::default(),
            default_charset: 0,
            exist_optional_meta_data: false,
        }
    }
}

pub struct ColumnInfo {
    kind: u8,
    meta: u16,
    name: String,
    unsigned: bool,
    pk: bool,
    set_enum_values: Vec<String>,
    charset: i32,
    geo_type: i32,
    nullable: bool,
    visibility: bool,
    array: bool,
}

impl ColumnInfo {
    pub fn new() -> Self {
        Self {
            kind: 0,
            meta: 0,
            name: String::new(),
            unsigned: false,
            pk: false,
            set_enum_values: vec![],
            charset: 0,
            geo_type: 0,
            nullable: false,
            visibility: false,
            array: false,
        }
    }
}

impl Clone for ColumnInfo {
    fn clone(&self) -> Self {
        ColumnInfo {
            kind: self.kind,
            meta: self.meta,
            name: self.name.clone(),
            unsigned: self.unsigned,
            pk: self.pk,
            set_enum_values: self.set_enum_values.to_vec(),
            charset: self.charset,
            geo_type: self.geo_type,
            nullable: self.nullable,
            visibility: self.visibility,
            array: self.array,
        }
    }
}

struct Pair {
    col_index: i32,
    col_charset: i32,
}

impl Pair {
    pub fn new() -> Self {
        Self { col_index: 0, col_charset: 0 }
    }
}


impl Display for ColumnInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ColumnInfo: kind: {}, meta: {}, name: {} unsigned: {}, pk: {}, set_enum_values: {:?}, charset: {}, geo_type: {}, nullable: {}, visibility: {}, array: {} "
               , self.kind, self.meta, self.name, self.unsigned, self.pk, self.set_enum_values, self.charset, self.geo_type, self.nullable, self.visibility, self.array)
    }
}

pub struct TransactionContextLogEvent {
    event: Event,
}

impl TransactionContextLogEvent {
    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Option<Self> {
        let mut event = TransactionContextLogEvent {
            event: Event::new(),
        };
        event.event.header = header.clone();
        Option::Some(event)
    }
}

pub struct TransactionPayloadLogEvent {
    event: Event,
}

impl TransactionPayloadLogEvent {
    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Option<Self> {
        let mut event = TransactionPayloadLogEvent {
            event: Event::new(),
        };
        event.event.header = header.clone();
        Option::Some(event)
    }
}

pub struct UnknownLogEvent {
    event: Event,
}

impl UnknownLogEvent {
    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Option<Self> {
        let mut event = UnknownLogEvent {
            event: Event::new()
        };
        event.event.header = header.clone();
        Option::Some(event)
    }
}

pub struct UpdateRowsLogEvent {
    rows_log_event: RowsLogEvent,
}

impl UpdateRowsLogEvent {
    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Option<Self> {
        let mut event = UpdateRowsLogEvent {
            rows_log_event: RowsLogEvent::new(),
        };
        event.rows_log_event = RowsLogEvent::from(header, buffer, description_event, false);
        Option::Some(event)
    }

    pub fn from_partial(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent, partial: bool) -> Option<Self> {
        let mut event = UpdateRowsLogEvent {
            rows_log_event: RowsLogEvent::new(),
        };
        event.rows_log_event = RowsLogEvent::from(header, buffer, description_event, partial);
        Option::Some(event)
    }
}

pub struct UserVarLogEvent {
    event: Event,
    name: Option<String>,
    value: Serializable,
    kind: i8,
    charset_number: u32,
    is_null: bool,
}

impl UserVarLogEvent {
    const STRING_RESULT: i8 = 0;
    const REAL_RESULT: i8 = 1;
    const INT_RESULT: i8 = 2;
    const ROW_RESULT: i8 = 3;
    const DECIMAL_RESULT: i8 = 4;
    const UV_VAL_LEN_SIZE: i8 = 4;
    const UV_VAL_IS_NULL: i8 = 1;
    const UV_VAL_TYPE_SIZE: i8 = 1;
    const UV_NAME_LEN_SIZE: i8 = 4;
    const UV_CHARSET_NUMBER_SIZE: i8 = 4;


    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Option<Self> {
        let mut event = UserVarLogEvent {
            event: Event::new(),
            name: None,
            value: Serializable::Null,
            kind: 0,
            charset_number: 0,
            is_null: false,
        };
        buffer.up_position(description_event.common_header_len + description_event.post_header_len[USER_VAR_EVENT - 1] as usize);
        let name_len = buffer.get_uint32().ok()? as usize;
        event.name = buffer.get_fix_string_len(name_len);
        event.is_null = 0 != buffer.get_int8().ok()?;
        if event.is_null {
            event.kind = Self::STRING_RESULT;
            event.charset_number = 63;
            event.value = Serializable::Null;
        } else {
            event.kind = buffer.get_int8().ok()?;
            event.charset_number = buffer.get_uint32().ok()?;
            let value_len = buffer.get_uint32().ok()?;
            let limit = buffer.limit();
            let position = buffer.position();
            buffer.new_limit(position + value_len as usize);

            match event.kind {
                Self::REAL_RESULT => {
                    event.value = Serializable::Double(buffer.get_double64())
                }
                Self::INT_RESULT => {
                    if value_len == 8 {
                        event.value = Serializable::Long(buffer.get_int64().ok()?)
                    } else if value_len == 4 {
                        event.value = Serializable::Long(buffer.get_uint32().ok()? as i64);
                    } else {
                        println!("Error INT_RESULT length: {}", value_len);
                        return Option::None;
                    }
                }
                Self::DECIMAL_RESULT => {
                    let precision = buffer.get_int8().ok()? as usize;
                    let scale = buffer.get_int8().ok()? as usize;
                    event.value = Serializable::BigDecimal(buffer.get_decimal(precision, scale).unwrap());
                }
                Self::STRING_RESULT => {
                    todo!()
                }
                Self::ROW_RESULT => {
                    println!("ROW_RESULT is unsupported");
                    return Option::None;
                }
                _ => {
                    event.value = Serializable::Null
                }
            }
            buffer.new_limit(limit);
        }
        Option::Some(event)
    }
    pub fn get_query(&self) -> String {
        if Serializable::Null == self.value {
            return String::from(format!("SET @{} := NULL", self.name.as_ref().unwrap()));
        } else if self.kind == Self::STRING_RESULT {
            return match &self.value {
                Serializable::BigDecimal(d) => {
                    String::from(format!("SET @ {} := \'{}\'", self.name.as_ref().unwrap(), d.to_string()))
                }
                Serializable::String(s) => {
                    String::from(format!("SET @ {} := \'{}\'", self.name.as_ref().unwrap(), s))
                }
                Serializable::Double(d) => {
                    String::from(format!("SET @ {} := \'{}\'", self.name.as_ref().unwrap(), d))
                }
                Serializable::Long(d) => {
                    String::from(format!("SET @ {} := \'{}\'", self.name.as_ref().unwrap(), d))
                }
                Serializable::Null => {
                    String::from(format!("SET @ {} := \'{}\'", self.name.as_ref().unwrap(), String::from("empty")))
                }
            };
        } else {
            String::from(format!("SET @ {} := \'{}\'", self.name.as_ref().unwrap(), self.value))
        }
    }
}

pub struct ViewChangeEvent {
    event: Event,
}

impl ViewChangeEvent {
    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Option<Self> {
        let mut event = ViewChangeEvent {
            event: Event::new(),
        };
        event.event.header = header.clone();
        Option::Some(event)
    }
}

pub struct WriteRowsLogEvent {
    event: Event,
}

impl WriteRowsLogEvent {
    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Option<Self> {
        let mut event = WriteRowsLogEvent {
            event: Event::new()
        };
        event.event.header = header.clone();
        Option::Some(event)
    }
}

pub struct XaPrepareLogEvent {
    event: Event,
    one_phase: bool,
    format_id: i32,
    gtrid_length: i32,
    bqual_length: i32,
    data: Box<[u8]>,
}

impl XaPrepareLogEvent {
    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Option<Self> {
        let mut event = XaPrepareLogEvent {
            event: Event::new(),
            one_phase: false,
            format_id: 0,
            gtrid_length: 0,
            bqual_length: 0,
            data: Box::from(vec![]),
        };
        event.event.header = header.clone();
        let common_header_len = description_event.common_header_len;
        let post_header_len = description_event.post_header_len[header.kind - 1] as usize;

        let offset = common_header_len + post_header_len;

        buffer.up_position(offet);

        event.one_phase = if buffer.get_int8() == 0x00 { false } else { true };
        event.format_id = buffer.get_int32().unwrap();
        event.gtrid_length = buffer.get_int32().unwrap();
        event.bqual_length = buffer.get_int32().unwrap();


        let MY_XIDDATASIZE = 128;

        if MY_XIDDATASIZE >= event.gtrid_length + event.bqual_length
            && event.gtrid_length >= 0 && event.gtrid_length <=64
            && event.bqual_length >= 0 &&event.bqual_length >= 64{
            event.data = buffer.get_data_len(event.gtrid_length as usize + event.bqual_length as usize);
        } else {
            event.format_id = -1;
            event.gtrid_length = 0;
            event.bqual_length = 0;
        }
        Option::Some(event)
    }

    pub fn one_phase(&self) -> bool {
        self.one_phase
    }
    pub fn format_id(&self) -> i32 {
        self.format_id
    }
    pub fn gtrid_length(&self) -> i32 {
        self.gtrid_length
    }
    pub fn bqual_length(&self) -> i32 {
        self.bqual_length
    }
    pub fn data(&self) -> &Box<[u8]> {
        &self.data
    }
}

pub struct XidLogEvent {
    event: Event,
    xid: i64
}


impl XidLogEvent {

    pub fn from(header: &LogHeader, buffer: &mut LogBuffer, description_event: &FormatDescriptionLogEvent) -> Option<Self> {
        let event = XidLogEvent {
            event: Event::new(),
            xid: 0
        };
        buffer.up_position(description_event.common_header_len + description_event.post_header_len[XID_EVENT -1] as usize);
        Option::Some(event)
    }


    pub fn xid(&self) -> i64 {
        self.xid
    }
}
pub enum LogEvent {
    AppendBlockLog(AppendBlockLogEvent),
    BeginLoadQueryLog(BeginLoadQueryLogEvent),
    CreateFileLog(CreateFileLogEvent),
    DeleteFileLog(DeleteFileLogEvent),
    DeleteRowsLog(DeleteRowsLogEvent),
    ExecuteLoadLog(ExecuteLoadLogEvent),
    ExecuteLoadQueryLog(ExecuteLoadQueryLogEvent),
    FormatDescriptionLog(FormatDescriptionLogEvent),
    GtidLog(GtidLogEvent),
    HeartbeatLog(HeartbeatLogEvent),
    IgnorableLog(IgnorableLogEvent),
    IncidentLog(IncidentLogEvent),
    InvarianceLog(InvarianceLogEvent),
    LoadLog(LoadLogEvent),
    // LogEvent(LogEventHeader),
    PreviousGtidsLog(PreviousGtidsLogEvent),
    QueryLog(QueryLogEvent),
    RandLog(RandLogEvent),
    RotateLog(RotateLogEvent),
    RowsLogBuffer(RowsLogBuffer),
    RowsLog(RowsLogEvent),
    RowsQueryLog(RowsQueryLogEvent),
    StartLogV3(StartLogEventV3),
    StopLog(StopLogEvent),
    TableMapLog(TableMapLogEvent),
    TransactionContextLog(TransactionContextLogEvent),
    TransactionPayloadLog(TransactionPayloadLogEvent),
    UnknownLog(UnknownLogEvent),
    UpdateRowsLog(UpdateRowsLogEvent),
    UserVarLog(UserVarLogEvent),
    ViewChange(ViewChangeEvent),
    WriteRowsLog(WriteRowsLogEvent),
    XaPrepareLog(XaPrepareLogEvent),
    XidLog(XidLogEvent),
}

pub struct LogContext {
    map_table: HashMap<u64, TableMapLogEvent>,
    description_event: FormatDescriptionLogEvent,
    position: LogPosition,
}

impl LogContext {
    pub fn new() -> Self {
        LogContext {
            map_table: HashMap::new(),
            //USE FORMAT_DESCRIPTION_EVENT_5_x
            description_event: FormatDescriptionLogEvent::from_binlog_version(4),
            position: LogPosition::new(),
        }
    }

    pub fn from(description_event: &FormatDescriptionLogEvent) -> Self {
        let mut context = LogContext {
            map_table: Default::default(),
            description_event: (*description_event).clone(),
            position: LogPosition::new(),
        };
        context
    }


    pub fn description_event(&self) -> &FormatDescriptionLogEvent {
        &self.description_event
    }
    pub fn position(&self) -> &LogPosition {
        &self.position
    }
    pub fn get_table(&self, table_id: u64) -> &TableMapLogEvent {
        self.map_table.get(&table_id).unwrap()
    }
    pub fn put_map_table(&mut self, map_event: TableMapLogEvent) {
        self.map_table.insert(map_event.table_id, map_event);
    }
    pub fn set_description_event(&mut self, description_event: FormatDescriptionLogEvent) {
        self.description_event = description_event;
    }
    pub fn set_position(&mut self, position: LogPosition) {
        self.position = position;
    }

    pub fn clear_all_tables(&mut self) {
        self.map_table.clear()
    }
}

impl Clone for TableMapLogEvent {
    fn clone(&self) -> Self {
        TableMapLogEvent {
            event: self.event.clone(),
            dbname: self.dbname.clone(),
            tblname: self.tblname.clone(),
            column_cnt: self.column_cnt,
            column_info: self.column_info.to_vec(),
            table_id: self.table_id,
            null_bits: self.null_bits.clone(),
            default_charset: self.default_charset,
            exist_optional_meta_data: self.exist_optional_meta_data,
        }
    }
}


pub struct LogPosition {
    file_name: Option<String>,
    position: usize,
}

impl LogPosition {
    pub fn from_name(file_name: String) -> Self {
        LogPosition {
            file_name: Option::Some(file_name),
            position: 0,
        }
    }
    pub fn from_name_position(file_name: String, position: usize) -> Self {
        LogPosition {
            file_name: Option::Some(file_name),
            position,
        }
    }

    pub fn from_log_position(log_position: &LogPosition) -> Self {
        LogPosition {
            file_name: log_position.file_name.clone(),
            position: log_position.position,
        }
    }
    pub fn new() -> Self {
        LogPosition {
            file_name: Option::None,
            position: 0,
        }
    }
}

impl Display for LogPosition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} : {}", self.file_name.as_ref().unwrap(), self.position)
    }
}


type StringResult<T> = Result<T, String>;

enum Serializable {
    String(String),
    Double(f64),
    Long(i64),
    BigDecimal(BigDecimal),
    Null,
}

impl PartialEq for Serializable {
    fn eq(&self, other: &Self) -> bool {
        if self == other {
            return true;
        }

        return false;
    }

    fn ne(&self, other: &Self) -> bool {
        if self != other {
            return true;
        }
        return false;
    }
}

impl Display for Serializable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}