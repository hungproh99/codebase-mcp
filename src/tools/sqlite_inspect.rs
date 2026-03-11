use anyhow::{Context, Result};
use rusqlite::types::ValueRef;
use rusqlite::{Connection, OpenFlags};
use serde_json::{Value, json};
use std::path::PathBuf;

use crate::common::insert_object_field;

pub fn schema() -> Value {
    json!({
        "name": "sqlite_inspect",
        "description": "Inspect a SQLite database in read-only mode.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "table": { "type": "string" },
                "sample_limit": { "type": "integer" },
                "sql": { "type": "string" },
                "include_tables": { "type": "boolean" }
            },
            "required": ["path"]
        }
    })
}

pub async fn execute(args: &Value) -> Result<Value> {
    let path_str = args
        .get("path")
        .and_then(|v| v.as_str())
        .context("Missing path")?;
    let path = PathBuf::from(path_str);
    if !path.exists() || !path.is_file() {
        return Err(anyhow::anyhow!(
            "File does not exist or is not a file: {}",
            path_str
        ));
    }

    let sample_limit = args
        .get("sample_limit")
        .and_then(|v| v.as_u64())
        .unwrap_or(10) as usize;
    let include_tables = args
        .get("include_tables")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    let connection = Connection::open_with_flags(
        &path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;

    let mut response = json!({
        "path": path_str
    });

    if include_tables {
        insert_object_field(&mut response, "tables", json!(list_tables(&connection)?));
    }

    if let Some(table) = args.get("table").and_then(|v| v.as_str()) {
        insert_object_field(
            &mut response,
            "table_info",
            json!({
                "table": table,
                "columns": table_columns(&connection, table)?,
                "sample_rows": sample_rows(&connection, table, sample_limit)?
            }),
        );
    }

    if let Some(sql) = args.get("sql").and_then(|v| v.as_str()) {
        ensure_read_only_sql(sql)?;
        insert_object_field(
            &mut response,
            "query_result",
            execute_read_only_query(&connection, sql)?,
        );
    }

    Ok(response)
}

fn list_tables(connection: &Connection) -> Result<Vec<Value>> {
    let mut statement = connection.prepare(
        "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(json!({
            "name": row.get::<_, String>(0)?,
            "type": row.get::<_, String>(1)?
        }))
    })?;

    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn table_columns(connection: &Connection, table: &str) -> Result<Vec<Value>> {
    let query = format!("PRAGMA table_info(\"{}\")", table.replace('"', "\"\""));
    let mut statement = connection.prepare(&query)?;
    let rows = statement.query_map([], |row| {
        Ok(json!({
            "cid": row.get::<_, i64>(0)?,
            "name": row.get::<_, String>(1)?,
            "data_type": row.get::<_, String>(2)?,
            "not_null": row.get::<_, i64>(3)? != 0,
            "default_value": row.get::<_, Option<String>>(4)?,
            "primary_key": row.get::<_, i64>(5)? != 0
        }))
    })?;

    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn sample_rows(connection: &Connection, table: &str, sample_limit: usize) -> Result<Vec<Value>> {
    let query = format!(
        "SELECT * FROM \"{}\" LIMIT {}",
        table.replace('"', "\"\""),
        sample_limit
    );
    execute_query_rows(connection, &query)
}

fn execute_read_only_query(connection: &Connection, sql: &str) -> Result<Value> {
    let rows = execute_query_rows(connection, sql)?;
    Ok(json!({
        "sql": sql,
        "rows": rows,
        "row_count_returned": rows.len()
    }))
}

fn execute_query_rows(connection: &Connection, sql: &str) -> Result<Vec<Value>> {
    let mut statement = connection.prepare(sql)?;
    let column_names: Vec<String> = statement
        .column_names()
        .iter()
        .map(|name| name.to_string())
        .collect();

    let rows = statement.query_map([], |row| {
        let mut object = serde_json::Map::new();
        for (index, name) in column_names.iter().enumerate() {
            let value = match row.get_ref(index)? {
                ValueRef::Null => Value::Null,
                ValueRef::Integer(value) => json!(value),
                ValueRef::Real(value) => json!(value),
                ValueRef::Text(value) => json!(String::from_utf8_lossy(value).to_string()),
                ValueRef::Blob(value) => json!(format!("blob({} bytes)", value.len())),
            };
            object.insert(name.clone(), value);
        }
        Ok(Value::Object(object))
    })?;

    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn ensure_read_only_sql(sql: &str) -> Result<()> {
    let normalized = sql.trim_start().to_ascii_lowercase();
    let read_only = normalized.starts_with("select ")
        || normalized.starts_with("pragma ")
        || normalized.starts_with("with ")
        || normalized.starts_with("explain ");

    if !read_only {
        return Err(anyhow::anyhow!(
            "sqlite_inspect only allows read-only SQL starting with SELECT, PRAGMA, WITH, or EXPLAIN"
        ));
    }

    Ok(())
}
