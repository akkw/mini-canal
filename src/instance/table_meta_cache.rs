use std::collections::HashMap;
use sql_parse::{CreateTable, DataType, parse_statements, ParseOptions, SQLDialect, Statement, Type};
use crate::channel::mysql_socket::MysqlConnection;
use crate::command::server::ResultSetPacket;
use crate::log::metadata::{FieldMeta, TableMeta};

pub struct TableMetaCache {
    table_meta: HashMap<String, TableMeta>,
    connection: Option<MysqlConnection>,
}


impl TableMetaCache {
    const COLUMN_NAME: &'static str = "Field";
    const COLUMN_TYPE: &'static str = "Type";
    const IS_NULLABLE: &'static str = "Null";
    const COLUMN_KEY: &'static str = "Key";
    const COLUMN_DEFAULT: &'static str = "Default";
    const EXTRA: &'static str = "Extra";
    const CREATE: &'static str = "CREATE";

    pub fn from(connection: Option<MysqlConnection>) -> TableMetaCache {
        TableMetaCache {
            table_meta: HashMap::new(),
            connection,
        }
    }


    pub fn get_table_meta_by_db(&mut self, full_name: &str) -> Option<TableMeta> {
        let table_meta = self.table_meta.get(full_name);
        return if let Option::Some(meta) = table_meta {
            Option::Some(meta.clone())
        } else {
            let packet = self.connection.as_mut().unwrap().query(format!("show create table {}", full_name).as_str());
            let mut names = full_name.split(".");
            let schema = names.next().unwrap();
            let table = names.next().unwrap();
            let meta = TableMeta::from(Option::Some(schema.to_string()), Option::Some(table.to_string()),
                                       Self::parse_table_meta(schema, table, &packet));
            let table_meta = self.table_meta.get(full_name);
            if let Option::Some(meta) = table_meta {
                Option::Some(meta.clone())
            } else {
                self.table_meta.insert(full_name.to_string(), meta);
                let table_meta = self.table_meta.get(full_name).unwrap();
                Option::Some(table_meta.clone())
            }
        };
    }

    pub fn parse_table_meta(schema: &str, table: &str, packet: &ResultSetPacket) -> Vec<FieldMeta> {
        return if packet.field_values().len() > 1 {
            let create_ddl = packet.field_values().get(1).unwrap();
            let meta = Self::parse_ddl(create_ddl, schema, table);
            meta.fields().clone()
        } else {
            vec![]
        };
    }

    fn parse_ddl(ddl: &str, schema: &str, table: &str) -> TableMeta {
        let mut meta = TableMeta::new();
        meta.set_schema(Option::Some(schema.to_string()));
        meta.set_table(Option::Some(table.to_string()));
        let options = ParseOptions::new().dialect(SQLDialect::MariaDB);
        let mut issues = Vec::new();
        let mut stmts = parse_statements(ddl, &mut issues, &options);
        let create: CreateTable = match stmts.pop() {
            Some(Statement::CreateTable(c)) => c,
            _ => panic!("We should get an create table statement")
        };
        let mut field_meta = vec![];

        for create_definition in create.create_definitions {

            match create_definition {

                sql_parse::CreateDefinition::ColumnDefinition {
                    identifier,
                    data_type,
                } => {
                    let kind;
                    match data_type.type_ {
                        Type::Boolean => {
                            kind = Option::Some(String::from("bool"))
                        }
                        Type::TinyInt(e) => {
                            let x = e.unwrap();
                            kind = Option::Some(String::from(format!("tinyint({})", x.0)))
                        }
                        Type::SmallInt(e) => {
                            let x = e.unwrap();
                            kind = Option::Some(String::from(format!("smallint({})", x.0)))
                        }
                        Type::Integer(e) => {
                            let x = e.unwrap();
                            kind = Option::Some(String::from(format!("integer({})", x.0)))
                        }
                        Type::Int(e) => {
                            let x = e.unwrap();
                            kind = Option::Some(String::from(format!("int({})", x.0)))
                        }
                        Type::BigInt(e) => {
                            let x = e.unwrap();
                            kind = Option::Some(String::from(format!("bigint({})", x.0)))
                        }
                        Type::Char(e) => {
                            let x = e.unwrap();
                            kind = Option::Some(String::from(format!("char({})", x.0)))
                        }
                        Type::VarChar(e) => {
                            let x = e.unwrap();
                            kind = Option::Some(String::from(format!("varchar({})", x.0)))
                        }
                        Type::TinyText(e) => {
                            let x = e.unwrap();
                            kind = Option::Some(String::from(format!("tinytext({})", x.0)))
                        }
                        Type::MediumText(e) => {
                            let x = e.unwrap();
                            kind = Option::Some(String::from(format!("mediumtext({})", x.0)))
                        }
                        Type::Text(e) => {
                            let x = e.unwrap();
                            kind = Option::Some(String::from(format!("text({})", x.0)))
                        }
                        Type::LongText(e) => {
                            let x = e.unwrap();
                            kind = Option::Some(String::from(format!("longtext({})", x.0)))
                        }
                        Type::Enum(e) => {
                            kind = Option::Some(String::from(format!("{}", e[0].value)))
                        }
                        Type::Set(e) => {
                            kind = Option::Some(String::from(format!("{}", e[0].value)))
                        }
                        Type::Float8 => {
                            kind = Option::Some(String::from(format!("float8")))
                        }
                        Type::Float(e) => {
                            let x = e.unwrap();
                            kind = Option::Some(String::from(format!("float({}.{})", x.0, x.1)))
                        }
                        Type::Double(e) => {
                            let x = e.unwrap();
                            kind = Option::Some(String::from(format!("double({}.{})", x.0, x.1)))
                        }
                        Type::Numeric(a, b, c) => {
                            kind = Option::Some(String::from(format!("numeric({}.{})", a, b)))
                        }
                        Type::DateTime(e) => {
                            kind = Option::Some(String::from(format!("datetime")))
                        }
                        Type::Timestamp(_) => {
                            kind = Option::Some(String::from(format!("timestamp")))
                        }
                        Type::Time(_) => {
                            kind = Option::Some(String::from(format!("time")))
                        }
                        Type::TinyBlob(_) => {
                            kind = Option::Some(String::from(format!("tinyblob")))
                        }
                        Type::MediumBlob(_) => {
                            kind = Option::Some(String::from(format!("mediumblob")))
                        }
                        Type::Date => {
                            kind = Option::Some(String::from(format!("date")))
                        }
                        Type::Blob(_) => {
                            kind = Option::Some(String::from(format!("blob")))
                        }
                        Type::LongBlob(_) => {
                            kind = Option::Some(String::from(format!("longblob")))
                        }
                        Type::VarBinary(_) => {
                            kind = Option::Some(String::from(format!("varbinary")))
                        }
                        Type::Binary(_) => {
                            kind = Option::Some(String::from(format!("binary")))
                        }
                    }
                    let meta = FieldMeta::from(Option::Some(identifier.value.to_string()), kind, false, false, Option::Some("".to_string()));
                    field_meta.push(meta);
                }
                _ => {}
            }

        }
        meta.set_fields(field_meta);
        meta
    }
}