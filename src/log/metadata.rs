use std::fmt::{Display, Formatter};
use protobuf::reflect::ProtobufValue;
use crate::StringResult;

pub struct EntryPosition {
    timestamp: Option<u64>,
    eventidentity_segment: Option<i32>,
    eventidentity_split: Option<char>,
    included: Option<bool>,
    journal_name: Option<String>,
    position: Option<u32>,
    server_id: Option<u32>,
    gtid: Option<String>,
}


impl Clone for EntryPosition {
    fn clone(&self) -> Self {
        Self {
            timestamp: self.timestamp,
            eventidentity_segment: self.eventidentity_segment,
            eventidentity_split: self.eventidentity_split,
            included: self.included,
            journal_name: self.journal_name.clone(),
            position: self.position,
            server_id: self.server_id,
            gtid: self.gtid.clone(),
        }
    }
}

impl EntryPosition {
    pub fn new() -> Self {
        Self {
            timestamp: Option::None,
            eventidentity_segment: Option::None,
            eventidentity_split: Option::None,
            included: Option::None,
            journal_name: Option::None,
            position: Option::None,
            server_id: Option::None,
            gtid: Option::None,
        }
    }

    fn _from_timestamp(timestamp: u64) -> EntryPosition {
        EntryPosition {
            timestamp: Option::Some(timestamp),
            eventidentity_segment: Option::None,
            eventidentity_split: Option::None,
            included: Option::None,
            journal_name: Option::None,
            position: Option::None,
            server_id: Option::None,
            gtid: Option::None,
        }
    }

    pub fn from_position(journal_name: String, position: u32) -> EntryPosition {
        EntryPosition {
            timestamp: Option::None,
            eventidentity_segment: Option::None,
            eventidentity_split: Option::None,
            included: Option::None,
            journal_name: Option::Some(journal_name),
            position: Option::Some(position),
            server_id: Option::None,
            gtid: Option::None,
        }
    }

    fn _from_journal_name_pos_timestamp(journal_name: String, position: u32, timestamp: u64) -> EntryPosition {
        EntryPosition {
            timestamp: Option::Some(timestamp),
            eventidentity_segment: Option::None,
            eventidentity_split: Option::None,
            included: Option::None,
            journal_name: Option::Some(journal_name),
            position: Option::Some(position),
            server_id: Option::None,
            gtid: Option::None,
        }
    }

    fn _from_server_id(journal_name: String, position: u32, timestamp: u64, server_id: u32) -> EntryPosition {
        EntryPosition {
            timestamp: Option::Some(timestamp),
            eventidentity_segment: Option::None,
            eventidentity_split: Option::None,
            included: Option::None,
            journal_name: Option::Some(journal_name),
            position: Option::Some(position),
            server_id: Option::Some(server_id),
            gtid: Option::None,
        }
    }


    pub fn timestamp(&self) -> Option<u64> {
        self.timestamp
    }
    pub fn eventidentity_segment(&self) -> Option<i32> {
        self.eventidentity_segment
    }
    pub fn eventidentity_split(&self) -> Option<char> {
        self.eventidentity_split
    }
    pub fn included(&self) -> Option<bool> {
        self.included
    }
    pub fn journal_name(&self) -> &Option<String> {
        &self.journal_name
    }
    pub fn position(&self) -> Option<u32> {
        self.position
    }
    pub fn server_id(&self) -> Option<u32> {
        self.server_id
    }
    pub fn gtid(&self) -> &Option<String> {
        &self.gtid
    }


    pub fn set_timestamp(&mut self, timestamp: Option<u64>) {
        self.timestamp = timestamp;
    }
    pub fn set_eventidentity_segment(&mut self, eventidentity_segment: Option<i32>) {
        self.eventidentity_segment = eventidentity_segment;
    }
    pub fn set_eventidentity_split(&mut self, eventidentity_split: Option<char>) {
        self.eventidentity_split = eventidentity_split;
    }
    pub fn set_included(&mut self, included: Option<bool>) {
        self.included = included;
    }
    pub fn set_journal_name(&mut self, journal_name: Option<String>) {
        self.journal_name = journal_name;
    }
    pub fn set_position(&mut self, position: Option<u32>) {
        self.position = position;
    }
    pub fn set_server_id(&mut self, server_id: Option<u32>) {
        self.server_id = server_id;
    }
    pub fn set_gtid(&mut self, gtid: Option<String>) {
        self.gtid = gtid;
    }
}

pub struct TableMeta {
    schema: Option<String>,
    table: Option<String>,
    fields: Vec<FieldMeta>,
    ddl: Option<String>,
}

impl Clone for TableMeta {
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            table: self.table.clone(),
            fields: self.fields.clone(),
            ddl: self.ddl.clone()
        }
    }
}

impl TableMeta {
    pub fn from(schema: Option<String>, table: Option<String>, fields: Vec<FieldMeta>) -> Self {
        Self {
            schema,
            table,
            fields,
            ddl: Option::None,
        }
    }


    pub fn new() -> Self {
        Self {
            schema: Option::None,
            table: Option::None,
            fields: Vec::new(),
            ddl: Option::None,
        }
    }

    pub fn get_field_meta_by_name(&mut self, name: &str) -> StringResult<&mut FieldMeta>{
        for meta in self.fields.iter_mut() {
            if  meta.column_name.as_ref().unwrap().to_lowercase().eq(name.to_lowercase().as_str()) {
                return Result::Ok(meta);
            }
        }
        Result::Err(format!("unknow column : {}", name))
    }

    pub fn get_primary_fields(&mut self) -> Vec<&mut FieldMeta> {
        let mut primarys = Vec::new();
        for meta in self.fields.iter_mut() {
            if  meta.key() {
                primarys.push(meta);
            }
        }
        return primarys;
    }

    pub fn add_field_meta(&mut self, field_meta: FieldMeta) {
        self.fields.push(field_meta);
    }


    pub fn schema(&self) -> &Option<String> {
        &self.schema
    }
    pub fn table(&self) -> &Option<String> {
        &self.table
    }
    pub fn fields(&self) -> &Vec<FieldMeta> {
        &self.fields
    }
    pub fn ddl(&self) -> &Option<String> {
        &self.ddl
    }


    pub fn set_schema(&mut self, schema: Option<String>) {
        self.schema = schema;
    }
    pub fn set_table(&mut self, table: Option<String>) {
        self.table = table;
    }
    pub fn set_fields(&mut self, fields: Vec<FieldMeta>) {
        self.fields = fields;
    }
    pub fn set_ddl(&mut self, ddl: Option<String>) {
        self.ddl = ddl;
    }
}


pub struct FieldMeta {
    column_name: Option<String>,
    column_type: Option<String>,
    nullable: bool,
    key: bool,
    default_value: Option<String>,
}

impl Clone for FieldMeta {
    fn clone(&self) -> Self {
        Self{
            column_name: self.column_name.clone(),
            column_type: self.column_type.clone(),
            nullable: self.nullable,
            key: self.key,
            default_value: self.default_value.clone()
        }
    }
}

impl Display for FieldMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "column_name: {}, column_type: {}, nullable: {},key: {}, default_value: {}",
               self.column_name.as_ref().unwrap(), self.column_type.as_ref().unwrap(), self.nullable,
               self.key, self.default_value.as_ref().unwrap())
    }
}

impl FieldMeta {
    pub fn from(column_name: Option<String>, column_type: Option<String>, nullable: bool, key: bool, default_value: Option<String>) -> Self {
        Self { column_name, column_type, nullable, key, default_value }
    }

    pub fn column_name(&self) -> &Option<String> {
        &self.column_name
    }
    pub fn column_type(&self) -> &Option<String> {
        &self.column_type
    }
    pub fn nullable(&self) -> bool {
        self.nullable
    }
    pub fn key(&self) -> bool {
        self.key
    }
    pub fn default_value(&self) -> &Option<String> {
        &self.default_value
    }


    pub fn set_column_name(&mut self, column_name: Option<String>) {
        self.column_name = column_name;
    }
    pub fn set_column_type(&mut self, column_type: Option<String>) {
        self.column_type = column_type;
    }
    pub fn set_nullable(&mut self, nullable: bool) {
        self.nullable = nullable;
    }
    pub fn set_key(&mut self, key: bool) {
        self.key = key;
    }
    pub fn set_default_value(&mut self, default_value: Option<String>) {
        self.default_value = default_value;
    }
}

