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
use crate::instance::log_buffer::LogBuffer;

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

/* Enumeration type for the different types of log events. */
pub const UNKNOWN_EVENT: u8 = 0;
pub const START_EVENT_V3: u8 = 1;
pub const QUERY_EVENT: u8 = 2;
pub const STOP_EVENT: u8 = 3;
pub const ROTATE_EVENT: u8 = 4;
pub const INTVAR_EVENT: u8 = 5;
pub const LOAD_EVENT: u8 = 6;
pub const SLAVE_EVENT: u8 = 7;
pub const CREATE_FILE_EVENT: u8 = 8;
pub const APPEND_BLOCK_EVENT: u8 = 9;
pub const EXEC_LOAD_EVENT: u8 = 10;
pub const DELETE_FILE_EVENT: u8 = 11;

/**
 * NEW_LOAD_EVENT is like LOAD_EVENT except that it has a longer sql_ex,
 * allowing multibyte TERMINATED BY etc; both types share the same class
 * (Load_log_event)
 */
pub const NEW_LOAD_EVENT: u8 = 12;
pub const RAND_EVENT: u8 = 13;
pub const USER_VAR_EVENT: u8 = 14;
pub const FORMAT_DESCRIPTION_EVENT: u8 = 15;
pub const XID_EVENT: u8 = 16;
pub const BEGIN_LOAD_QUERY_EVENT: u8 = 17;
pub const EXECUTE_LOAD_QUERY_EVENT: u8 = 18;
pub const TABLE_MAP_EVENT: u8 = 19;

/**
 * These event numbers were used for 5.1.0 to 5.1.15 and are therefore
 * obsolete.
 */
pub const PRE_GA_WRITE_ROWS_EVENT: u8 = 20;
pub const PRE_GA_UPDATE_ROWS_EVENT: u8 = 21;
pub const PRE_GA_DELETE_ROWS_EVENT: u8 = 22;

/**
 * These event numbers are used from 5.1.16 and forward
 */
pub const WRITE_ROWS_EVENT_V1: u8 = 23;
pub const UPDATE_ROWS_EVENT_V1: u8 = 24;
pub const DELETE_ROWS_EVENT_V1: u8 = 25;

/**
 * Something out of the ordinary happened on the master
 */
pub const INCIDENT_EVENT: u8 = 26;

/**
 * Heartbeat event to be send by master at its idle time to ensure master's
 * online status to slave
 */
pub const HEARTBEAT_LOG_EVENT: u8 = 27;

/**
 * In some situations, it is necessary to send over ignorable data to the
 * slave: data that a slave can handle in  there is code for handling
 * it, but which can be ignored if it is not recognized.
 */
pub const IGNORABLE_LOG_EVENT: u8 = 28;
pub const ROWS_QUERY_LOG_EVENT: u8 = 29;

/** Version 2 of the Row events */
pub const WRITE_ROWS_EVENT: u8 = 30;
pub const UPDATE_ROWS_EVENT: u8 = 31;
pub const DELETE_ROWS_EVENT: u8 = 32;
pub const GTID_LOG_EVENT: u8 = 33;
pub const ANONYMOUS_GTID_LOG_EVENT: u8 = 34;

pub const PREVIOUS_GTIDS_LOG_EVENT: u8 = 35;

/* MySQL 5.7 events */
pub const TRANSACTION_CONTEXT_EVENT: u8 = 36;

pub const VIEW_CHANGE_EVENT: u8 = 37;

/* Prepared XA transaction terminal event similar to Xid */
pub const XA_PREPARE_LOG_EVENT: u8 = 38;

/**
 * Extension of UPDATE_ROWS_EVENT, allowing partial values according to
 * binlog_row_value_options.
 */
pub const PARTIAL_UPDATE_ROWS_EVENT: u8 = 39;

/* mysql 8.0.20 */
pub const TRANSACTION_PAYLOAD_EVENT: u8 = 40;

pub const MYSQL_ENUM_END_EVENT: u8 = 41;

// mariaDb 5.5.34
/* New MySQL/Sun events are to be added right above this comment */
pub const MYSQL_EVENTS_END: u8 = 49;

pub const MARIA_EVENTS_BEGIN: u8 = 160;
/* New Maria event numbers start from here */
pub const ANNOTATE_ROWS_EVENT: u8 = 160;
/*
 * Binlog checkpoint event. Used for XA crash recovery on the master, not
 * used in replication. A binlog checkpoint event specifies a binlog file
 * such that XA crash recovery can start from that file - and it is
 * guaranteed to find all XIDs that are prepared in storage engines but not
 * yet committed.
 */
pub const BINLOG_CHECKPOINT_EVENT: u8 = 161;
/*
 * Gtid event. For global transaction ID, used to start a new event group,
 * instead of the old BEGIN query event, and also to mark stand-alone
 * events.
 */
pub const GTID_EVENT: u8 = 162;
/*
 * Gtid list event. Logged at the start of every binlog, to record the
 * current replication state. This consists of the last GTID seen for each
 * replication domain.
 */
pub const GTID_LIST_EVENT: u8 = 163;

pub const START_ENCRYPTION_EVENT: u8 = 164;

/** end marker */
pub const ENUM_END_EVENT: u8 = 165;

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


struct LogEvent {}

impl LogEvent {
    fn get_type_name(t: u8) -> String {
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
            _ =>  format!("Unknown type=> {}", t)
        }
    }
}


struct LogHeader {
    kind: u16,
    log_pos:i32,
    when: i32,
    event_len: i32,
    server_id: i32,
    flags: i32,
    checksum_alg: i32,
    crc: i32,
    log_file_name: Option<String>,
}


struct FormatDescriptionLogEvent {

}


impl LogHeader {
    fn from(kind: u16) -> LogHeader{
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
        }
    }

    fn from_buffer(buffer: LogBuffer, event: FormatDescriptionLogEvent) {

    }

}