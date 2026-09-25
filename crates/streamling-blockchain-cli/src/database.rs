use crate::{
    clickhouse::ClickHouse,
    config::ProjectConfig,
    output::{CodedError, ErrorCode},
};
use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OpenFlags, functions::FunctionFlags, types::ValueRef};
use serde_json::{Map, Value, json};
use std::path::Path;

/// Width of the zero-padded digit run in an `int_sortkey` key: `2^256 - 1` has 78 decimal digits,
/// the longest value the CLI ever stores (uint256 max; int256 magnitudes are smaller).
const SORTKEY_DIGITS: usize = 78;

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum BackendKind {
    Sqlite,
    Clickhouse,
}

/// The configured sink that read commands query. SQLite wins when both sinks are enabled.
pub fn resolve(config: &ProjectConfig, requested: Option<BackendKind>) -> Result<BackendKind> {
    match requested {
        Some(BackendKind::Sqlite) if !config.sinks.sqlite => bail!(
            CodedError::new(
                ErrorCode::Validation,
                "this project has no SQLite sink; use --backend clickhouse"
            )
            .next("streamling-blockchain schema --backend clickhouse")
        ),
        Some(BackendKind::Clickhouse) if config.sinks.clickhouse.is_none() => bail!(
            CodedError::new(
                ErrorCode::Validation,
                "this project has no ClickHouse sink; use --backend sqlite"
            )
            .next("streamling-blockchain schema --backend sqlite")
        ),
        Some(kind) => Ok(kind),
        None if config.sinks.sqlite => Ok(BackendKind::Sqlite),
        None => Ok(BackendKind::Clickhouse),
    }
}

pub enum Backend {
    Sqlite(Connection),
    ClickHouse { client: ClickHouse, table: String },
}

impl Backend {
    pub fn open(
        root: &Path,
        config: &ProjectConfig,
        requested: Option<BackendKind>,
    ) -> Result<Self> {
        match resolve(config, requested)? {
            BackendKind::Sqlite => Ok(Self::Sqlite(open(&config.absolute_database(root))?)),
            BackendKind::Clickhouse => {
                let sink = config
                    .sinks
                    .clickhouse
                    .as_ref()
                    .context("ClickHouse sink is not configured")?;
                Ok(Self::ClickHouse {
                    client: ClickHouse::from_env(sink.database.as_deref())?,
                    table: sink.table.clone(),
                })
            }
        }
    }

    pub async fn query(&self, sql: &str, max_rows: usize) -> Result<Value> {
        match self {
            Self::Sqlite(conn) => query(conn, sql, max_rows),
            Self::ClickHouse { client, .. } => client.query(sql, max_rows).await,
        }
    }

    pub async fn schema(&self) -> Result<Value> {
        match self {
            Self::Sqlite(conn) => schema(conn),
            Self::ClickHouse { client, table } => {
                let mut value = client.schema().await?;
                value["events_table"] = client.qualified(table).into();
                value["dialect_notes"] = json!([
                    "Decoded event fields are JSON in fields_json; read them with JSONExtractString(fields_json, 'field').",
                    "Tables are ReplacingMergeTree; read current rows with FINAL WHERE is_deleted = 0.",
                    "Per-event <alias>__<event> tables in semantic.toml exist only in the SQLite sink; filter events by contract_alias and event_name instead."
                ]);
                Ok(value)
            }
        }
    }
}

pub fn open(path: &Path) -> Result<Connection> {
    if !path.exists() {
        bail!(
            CodedError::new(
                ErrorCode::NotFound,
                "database does not exist yet: run `streamling-blockchain dev`"
            )
            .next("streamling-blockchain dev")
        );
    }
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("open {} read-only", path.display()))?;
    register_int_sortkey(&conn)?;
    Ok(conn)
}

/// Registers `int_sortkey(value)`, converting a decoded int256/uint256 decimal string (or a
/// SQLite INTEGER) into fixed-width TEXT that sorts by byte order the same way the value sorts
/// numerically. `data` stores decoded integers as decimal strings, so plain `ORDER BY` or
/// `CAST(... AS INTEGER)` sorts them wrong or overflows; see `int_sortkey` for the encoding.
fn register_int_sortkey(conn: &Connection) -> Result<()> {
    conn.create_scalar_function(
        "int_sortkey",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| match ctx.get_raw(0) {
            ValueRef::Null => Ok(None),
            ValueRef::Integer(v) => int_sortkey(&v.to_string())
                .map(Some)
                .map_err(|e| rusqlite::Error::UserFunctionError(e.into())),
            ValueRef::Text(text) => {
                let text = std::str::from_utf8(text)
                    .map_err(|e| rusqlite::Error::UserFunctionError(e.into()))?;
                int_sortkey(text)
                    .map(Some)
                    .map_err(|e| rusqlite::Error::UserFunctionError(e.into()))
            }
            other => Err(rusqlite::Error::UserFunctionError(
                format!("int_sortkey expects an integer or a decimal string, got {other:?}").into(),
            )),
        },
    )
    .context("register int_sortkey")
}

/// Encodes a decimal integer (optional leading `-`) as fixed-width TEXT whose byte order matches
/// signed numeric order across the int256/uint256 range: `"1"` + the zero-padded magnitude for
/// non-negative values, `"0"` + the nines' complement of the zero-padded magnitude for negatives.
fn int_sortkey(input: &str) -> Result<String> {
    let (negative, digits) = match input.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, input),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        bail!("int_sortkey: not a decimal integer: {input:?}");
    }
    let digits = digits.trim_start_matches('0');
    if digits.len() > SORTKEY_DIGITS {
        bail!("int_sortkey: {input:?} has more than {SORTKEY_DIGITS} digits");
    }
    if digits.is_empty() {
        // "-0" normalises to zero, which sorts with the non-negative values.
        return Ok(format!("1{}", "0".repeat(SORTKEY_DIGITS)));
    }
    let padded = format!("{digits:0>SORTKEY_DIGITS$}");
    if !negative {
        return Ok(format!("1{padded}"));
    }
    let complement: String = padded.bytes().map(|b| (b'9' - b + b'0') as char).collect();
    Ok(format!("0{complement}"))
}

pub fn query(conn: &Connection, sql: &str, max_rows: usize) -> Result<Value> {
    let mut statement = conn.prepare(sql).context("prepare SQL")?;
    if statement.column_count() == 0 {
        let changed = statement.execute([])?;
        return Ok(json!({"columns": [], "rows": [], "changed": changed}));
    }
    let columns = statement
        .column_names()
        .iter()
        .map(|s| (*s).to_owned())
        .collect::<Vec<_>>();
    let mut rows = statement.query([])?;
    let mut output = Vec::new();
    let mut truncated = false;
    while let Some(row) = rows.next()? {
        if output.len() == max_rows {
            truncated = true;
            break;
        }
        let mut object = Map::new();
        for (index, name) in columns.iter().enumerate() {
            object.insert(name.clone(), value_to_json(row.get_ref(index)?));
        }
        output.push(Value::Object(object));
    }
    Ok(json!({"columns": columns, "rows": output, "truncated": truncated}))
}

fn value_to_json(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(v) => v.into(),
        ValueRef::Real(v) => json!(v),
        ValueRef::Text(v) => String::from_utf8_lossy(v).into_owned().into(),
        ValueRef::Blob(v) => format!("0x{}", hex::encode(v)).into(),
    }
}

pub fn schema(conn: &Connection) -> Result<Value> {
    let mut statement = conn.prepare(
        "SELECT name, type, sql FROM sqlite_schema WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )?;
    let entries = statement
        .query_map([], |row| {
            Ok(json!({
                "name": row.get::<_, String>(0)?,
                "type": row.get::<_, String>(1)?,
                "sql": row.get::<_, Option<String>>(2)?
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(json!({
        "backend": "sqlite",
        "objects": entries,
        "dialect_notes": [
            "Decoded integer event fields are decimal strings; sort or compare them with int_sortkey(value), not CAST(value AS INTEGER) or CAST(value AS REAL), which silently break above i64 or lose precision on int256/uint256."
        ]
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClickHouseSinkConfig, ContractConfig, SinkConfig};
    use std::path::PathBuf;

    fn config(sqlite: bool, clickhouse: bool) -> ProjectConfig {
        ProjectConfig {
            chain: "ethereum".into(),
            chain_id: 1,
            rpc_url: "http://localhost".into(),
            rpc_url_env: None,
            database: PathBuf::from("events.db"),
            start_block: 0,
            end_block: None,
            confirmations: 12,
            window: 100,
            index_blocks: false,
            index_transactions: false,
            contracts: vec![ContractConfig {
                alias: "token".into(),
                address: "0x1111111111111111111111111111111111111111".into(),
                abi: PathBuf::from("abi.json"),
            }],
            discovery_rules: vec![],
            sinks: SinkConfig {
                sqlite,
                clickhouse: clickhouse.then(|| ClickHouseSinkConfig {
                    table: "events".into(),
                    compression: None,
                    database: None,
                }),
            },
        }
    }

    #[test]
    fn resolves_the_backend_from_configured_sinks() {
        assert_eq!(
            resolve(&config(true, true), None).unwrap(),
            BackendKind::Sqlite
        );
        assert_eq!(
            resolve(&config(false, true), None).unwrap(),
            BackendKind::Clickhouse
        );
        assert_eq!(
            resolve(&config(true, true), Some(BackendKind::Clickhouse)).unwrap(),
            BackendKind::Clickhouse
        );
        assert!(resolve(&config(true, false), Some(BackendKind::Clickhouse)).is_err());
        assert!(resolve(&config(false, true), Some(BackendKind::Sqlite)).is_err());
    }

    #[test]
    fn int_sortkey_orders_zero_and_small_values() {
        assert_eq!(int_sortkey("0").unwrap(), int_sortkey("-0").unwrap());
        assert!(int_sortkey("9").unwrap() < int_sortkey("10").unwrap());
        assert!(int_sortkey("-1").unwrap() < int_sortkey("0").unwrap());
        assert!(int_sortkey("-10").unwrap() < int_sortkey("-9").unwrap());
    }

    #[test]
    fn int_sortkey_normalises_leading_zeros() {
        assert_eq!(int_sortkey("007").unwrap(), int_sortkey("7").unwrap());
    }

    #[test]
    fn int_sortkey_covers_int256_and_uint256_extremes() {
        let uint256_max = "1157920892373161954235709850086879078532699846656405640394575840079131296399\
35";
        // 2^256 - 1: the largest value the CLI ever stores, at exactly 78 digits.
        assert_eq!(uint256_max.len(), 78);
        let max_key = int_sortkey(uint256_max).unwrap();
        assert!(int_sortkey("10").unwrap() < max_key);
        assert!(max_key.starts_with('1'));

        let int256_min =
            "-57896044618658097711785492504343953926634992332820282019728792003956564819968";
        // -2^255: the most negative int256 value; must sort before every other negative in tests.
        let min_key = int_sortkey(int256_min).unwrap();
        assert!(min_key < int_sortkey("-1").unwrap());
        assert!(min_key < int_sortkey(uint256_max).unwrap());
    }

    #[test]
    fn int_sortkey_rejects_garbage_and_oversized_input() {
        assert!(int_sortkey("").is_err());
        assert!(int_sortkey("abc").is_err());
        assert!(int_sortkey("1.5").is_err());
        let too_long = "1".repeat(SORTKEY_DIGITS + 1);
        assert!(int_sortkey(&too_long).is_err());
    }

    #[test]
    fn int_sortkey_sql_function_handles_null_and_orders_a_query() {
        let conn = Connection::open_in_memory().unwrap();
        register_int_sortkey(&conn).unwrap();

        let null: Option<String> = conn
            .query_row("SELECT int_sortkey(NULL)", [], |row| row.get(0))
            .unwrap();
        assert_eq!(null, None);

        assert!(
            conn.query_row("SELECT int_sortkey('not-a-number')", [], |row| row
                .get::<_, String>(0))
                .is_err()
        );

        conn.execute_batch(
            "CREATE TABLE events (data TEXT NOT NULL);
             INSERT INTO events (data) VALUES
                ('{\"value\": \"10\"}'),
                ('{\"value\": \"9\"}'),
                ('{\"value\": \"-1\"}'),
                ('{\"value\": \"115792089237316195423570985008687907853269984665640564039457584007913129639935\"}');",
        )
        .unwrap();
        let mut statement = conn
            .prepare("SELECT json_extract(data, '$.value') AS value FROM events ORDER BY int_sortkey(value)")
            .unwrap();
        let ordered = statement
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            ordered,
            vec![
                "-1".to_string(),
                "9".to_string(),
                "10".to_string(),
                "115792089237316195423570985008687907853269984665640564039457584007913129639935"
                    .to_string(),
            ]
        );
    }
}
