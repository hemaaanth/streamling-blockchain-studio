use crate::{
    config::ProjectConfig,
    output::{CodedError, ErrorCode},
    project,
};
use anyhow::{Context, Result, bail};
use serde_json::{Map, Value, json, value::RawValue};

pub const URL_ENV: &str = "STREAMLING__CLICKHOUSE_SINK__URL";
pub const USER_ENV: &str = "STREAMLING__CLICKHOUSE_SINK__USER";
pub const PASSWORD_ENV: &str = "STREAMLING__CLICKHOUSE_SINK__PASSWORD";
pub const DATABASE_ENV: &str = "STREAMLING__CLICKHOUSE_SINK__DATABASE";

/// HTTP client for the ClickHouse server that Streamling's ClickHouse sink writes to.
/// It reuses the sink's `STREAMLING__CLICKHOUSE_SINK__*` environment variables.
#[derive(Clone)]
pub struct ClickHouse {
    url: String,
    user: String,
    password: String,
    pub database: String,
    http: reqwest::Client,
}

impl ClickHouse {
    /// `database` is the project's configured ClickHouse database, when set.
    pub fn from_env(database: Option<&str>) -> Result<Self> {
        let url = std::env::var(URL_ENV)
            .with_context(|| format!("set {URL_ENV} to the ClickHouse HTTP URL"))?;
        let database = match database {
            Some(database) => database.to_owned(),
            None => std::env::var(DATABASE_ENV).unwrap_or_else(|_| "default".into()),
        };
        Self::new(
            &url,
            &std::env::var(USER_ENV).unwrap_or_else(|_| "default".into()),
            &std::env::var(PASSWORD_ENV).unwrap_or_default(),
            &database,
        )
    }

    pub fn new(url: &str, user: &str, password: &str, database: &str) -> Result<Self> {
        Ok(Self {
            url: url.trim_end_matches('/').to_owned(),
            user: user.to_owned(),
            password: password.to_owned(),
            database: database.to_owned(),
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(300))
                .build()?,
        })
    }

    pub fn qualified(&self, table: &str) -> String {
        format!("{}.{table}", self.database)
    }

    /// Runs one read-only statement. The server enforces `readonly=1`, which also works
    /// for users whose profile is already read-only. Reading stops after `max_rows`.
    pub async fn query(&self, sql: &str, max_rows: usize) -> Result<Value> {
        let mut response = self
            .send(
                sql,
                &[
                    ("readonly", "1"),
                    ("default_format", "JSONCompactEachRowWithNamesAndTypes"),
                ],
            )
            .await?;
        let mut lines = LineBuffer::default();
        let mut result = RowReader::new(max_rows);
        while let Some(chunk) = response.chunk().await.context("read ClickHouse response")? {
            lines.push(&chunk);
            while let Some(line) = lines.next_line() {
                if !result.push(&line)? {
                    return result.finish(true);
                }
            }
        }
        if let Some(line) = lines.rest() {
            result.push(&line)?;
        }
        result.finish(false)
    }

    /// Runs one statement that may write, such as DDL or an audit insert.
    pub async fn execute(&self, sql: &str) -> Result<()> {
        self.send(sql, &[]).await?;
        Ok(())
    }

    /// Inserts rows into `table` in the configured database.
    pub async fn insert_json_rows(&self, table: &str, rows: &[Value]) -> Result<()> {
        let mut body = format!("INSERT INTO {} FORMAT JSONEachRow\n", self.qualified(table));
        for row in rows {
            body.push_str(&serde_json::to_string(row)?);
            body.push('\n');
        }
        self.execute(&body).await
    }

    pub async fn schema(&self) -> Result<Value> {
        let rows = self
            .query(
                &format!(
                    "SELECT name, engine, sorting_key, create_table_query FROM system.tables WHERE database = {} ORDER BY name",
                    quote_string(&self.database)
                ),
                10_000,
            )
            .await?;
        let objects = rows["rows"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|row| {
                let engine = row["engine"].as_str().unwrap_or_default();
                json!({
                    "name": row["name"],
                    "type": if engine.ends_with("View") { "view" } else { "table" },
                    "engine": engine,
                    "sorting_key": row["sorting_key"],
                    "sql": row["create_table_query"],
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({"backend": "clickhouse", "database": self.database, "objects": objects}))
    }

    pub async fn table_exists(&self, table: &str) -> Result<bool> {
        let rows = self
            .query(
                &format!(
                    "SELECT count() AS tables FROM system.tables WHERE database = {} AND name = {}",
                    quote_string(&self.database),
                    quote_string(table)
                ),
                1,
            )
            .await?;
        Ok(rows["rows"][0]["tables"].as_u64() == Some(1))
    }

    async fn send(&self, sql: &str, settings: &[(&str, &str)]) -> Result<reqwest::Response> {
        let response = self
            .http
            .post(format!("{}/", self.url))
            .query(&[("database", self.database.as_str())])
            .query(settings)
            .basic_auth(&self.user, Some(&self.password))
            .body(sql.to_owned())
            .send()
            .await
            .with_context(|| format!("connect to ClickHouse at {}", self.url))?;
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            bail!(CodedError::new(
                response_code(status.as_u16(), &text),
                format!("ClickHouse returned {status}: {}", text.trim())
            ));
        }
        Ok(response)
    }
}

/// Creates the configured database and sink tables before Streamling starts, so the sink
/// writes into our sorting key instead of creating tables ordered by the primary key alone.
/// Returns one warning per existing table whose sorting key differs; `CREATE TABLE IF NOT
/// EXISTS` never changes an existing table.
pub async fn apply_schema(config: &ProjectConfig) -> Result<Vec<String>> {
    let database = config
        .sinks
        .clickhouse
        .as_ref()
        .and_then(|sink| sink.database.as_deref());
    apply_schema_with(&ClickHouse::from_env(database)?, config).await
}

pub(crate) async fn apply_schema_with(
    client: &ClickHouse,
    config: &ProjectConfig,
) -> Result<Vec<String>> {
    let sink = config
        .sinks
        .clickhouse
        .as_ref()
        .context("this project has no ClickHouse sink")?;
    let statements = project::clickhouse_statements(
        sink.database.as_deref(),
        &sink.table,
        config.index_blocks,
        config.index_transactions,
    );
    for statement in statements.iter().filter(|s| s.sorting_key.is_none()) {
        // The target database may not exist yet, so create it from `default`.
        let admin = ClickHouse {
            database: "default".into(),
            ..client.clone()
        };
        admin.execute(&statement.sql).await?;
    }
    let mut warnings = Vec::new();
    for statement in statements {
        let Some(expected) = statement.sorting_key else {
            continue;
        };
        client.execute(&statement.sql).await?;
        let rows = client
            .query(
                &format!(
                    "SELECT sorting_key FROM system.tables WHERE database = {} AND name = {}",
                    quote_string(&client.database),
                    quote_string(&statement.name)
                ),
                1,
            )
            .await?;
        let actual = rows["rows"][0]["sorting_key"].as_str().unwrap_or_default();
        if actual != expected {
            warnings.push(format!(
                "{} is ordered by ({actual}), not ({expected}); recreate it to use the current sorting key",
                client.qualified(&statement.name)
            ));
        }
    }
    Ok(warnings)
}

/// Maps a failed ClickHouse HTTP response to an error code by status and exception name.
fn response_code(status: u16, text: &str) -> ErrorCode {
    match status {
        _ if text.contains("AUTHENTICATION_FAILED") || text.contains("REQUIRED_PASSWORD") => {
            ErrorCode::MissingCredentials
        }
        401 => ErrorCode::MissingCredentials,
        408 | 429 | 502..=504 => ErrorCode::TransientDependency,
        400..=499 => ErrorCode::Validation,
        _ if text.contains("(READONLY)") => ErrorCode::Validation,
        _ => ErrorCode::Internal,
    }
}

pub fn quote_string(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

#[derive(Default)]
struct LineBuffer {
    pending: Vec<u8>,
}

impl LineBuffer {
    fn push(&mut self, bytes: &[u8]) {
        self.pending.extend_from_slice(bytes);
    }

    fn next_line(&mut self) -> Option<String> {
        let end = self.pending.iter().position(|byte| *byte == b'\n')?;
        let line = self.pending.drain(..=end).collect::<Vec<_>>();
        Some(String::from_utf8_lossy(&line[..end]).into_owned())
    }

    fn rest(&mut self) -> Option<String> {
        let rest = std::mem::take(&mut self.pending);
        (!rest.is_empty()).then(|| String::from_utf8_lossy(&rest).into_owned())
    }
}

/// Decodes `JSONCompactEachRowWithNamesAndTypes` lines into the same shape as the SQLite
/// query output: `{"columns": [...], "rows": [{...}], "truncated": bool}`.
struct RowReader {
    max_rows: usize,
    columns: Option<Vec<String>>,
    types: Option<Vec<String>>,
    rows: Vec<Value>,
}

impl RowReader {
    fn new(max_rows: usize) -> Self {
        Self {
            max_rows,
            columns: None,
            types: None,
            rows: Vec::new(),
        }
    }

    /// Returns false once another row arrives after `max_rows` rows.
    fn push(&mut self, line: &str) -> Result<bool> {
        if line.trim().is_empty() {
            return Ok(true);
        }
        if self.columns.is_none() {
            self.columns =
                Some(serde_json::from_str(line).context("parse ClickHouse column names")?);
            return Ok(true);
        }
        if self.types.is_none() {
            self.types = Some(serde_json::from_str(line).context("parse ClickHouse column types")?);
            return Ok(true);
        }
        if self.rows.len() == self.max_rows {
            return Ok(false);
        }
        let values: Vec<Box<RawValue>> =
            serde_json::from_str(line).context("parse ClickHouse row")?;
        let columns = self.columns.as_deref().unwrap_or_default();
        let types = self.types.as_deref().unwrap_or_default();
        let mut object = Map::new();
        for (index, raw) in values.iter().enumerate() {
            let name = columns
                .get(index)
                .cloned()
                .unwrap_or_else(|| index.to_string());
            let kind = types.get(index).map(String::as_str).unwrap_or_default();
            object.insert(name, decode_value(raw.get(), kind)?);
        }
        self.rows.push(Value::Object(object));
        Ok(true)
    }

    fn finish(self, truncated: bool) -> Result<Value> {
        Ok(json!({
            "columns": self.columns.unwrap_or_default(),
            "rows": self.rows,
            "truncated": truncated,
        }))
    }
}

/// Keeps wide integers and decimals exact as strings, and turns 64-bit integers that older
/// servers quote into JSON numbers.
fn decode_value(raw: &str, kind: &str) -> Result<Value> {
    let base = unwrap_type(kind);
    let is_number_literal = raw.starts_with(|c: char| c == '-' || c.is_ascii_digit());
    if is_number_literal
        && (base.starts_with("Decimal")
            || matches!(base, "Int128" | "UInt128" | "Int256" | "UInt256"))
    {
        return Ok(Value::String(raw.to_owned()));
    }
    let value: Value = serde_json::from_str(raw).context("parse ClickHouse value")?;
    if let (Value::String(text), "Int64" | "UInt64") = (&value, base) {
        if let Ok(number) = text.parse::<i64>() {
            return Ok(number.into());
        }
        if let Ok(number) = text.parse::<u64>() {
            return Ok(number.into());
        }
    }
    Ok(value)
}

fn unwrap_type(kind: &str) -> &str {
    let mut base = kind.trim();
    while let Some(inner) = ["LowCardinality(", "Nullable("]
        .iter()
        .find_map(|wrapper| base.strip_prefix(wrapper)?.strip_suffix(')'))
    {
        base = inner;
    }
    base
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::config::{ClickHouseSinkConfig, ContractConfig, SinkConfig};
    use std::path::PathBuf;

    /// A client for the ClickHouse server named by `STREAMLING_BLOCKCHAIN_TEST_CLICKHOUSE_URL`
    /// (plus optional `_USER` and `_PASSWORD`). Integration tests skip when it is unset.
    pub(crate) fn test_client(database: &str) -> Option<ClickHouse> {
        let url = std::env::var("STREAMLING_BLOCKCHAIN_TEST_CLICKHOUSE_URL").ok()?;
        let user = std::env::var("STREAMLING_BLOCKCHAIN_TEST_CLICKHOUSE_USER")
            .unwrap_or_else(|_| "default".into());
        let password =
            std::env::var("STREAMLING_BLOCKCHAIN_TEST_CLICKHOUSE_PASSWORD").unwrap_or_default();
        Some(ClickHouse::new(&url, &user, &password, database).unwrap())
    }

    pub(crate) fn clickhouse_config(database: &str, table: &str) -> ProjectConfig {
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
            index_blocks: true,
            index_transactions: false,
            contracts: vec![ContractConfig {
                alias: "token".into(),
                address: "0x1111111111111111111111111111111111111111".into(),
                abi: PathBuf::from("abi.json"),
            }],
            discovery_rules: vec![],
            sinks: SinkConfig {
                sqlite: false,
                clickhouse: Some(ClickHouseSinkConfig {
                    table: table.into(),
                    compression: None,
                    database: Some(database.into()),
                }),
            },
        }
    }

    pub(crate) fn event_row(event_id: &str, block_number: u64) -> Value {
        json!({
            "event_id": event_id,
            "chain_id": 1,
            "contract_alias": "token",
            "event_name": "Transfer",
            "address": "0x1111111111111111111111111111111111111111",
            "block_number": block_number,
            "block_hash": "0xaaa",
            "block_timestamp": 1_700_000_000,
            "tx_hash": "0xbbb",
            "log_index": 0,
            "topic0": "0xddf2",
            "fields_json": "{\"value\":\"1\"}",
            "discovered_address": null,
            "discovery_rule": null,
            "is_deleted": 0,
        })
    }

    #[tokio::test]
    async fn applies_schema_and_reads_through_final_read_only() {
        let database = format!("sbs_test_schema_{}", std::process::id());
        let Some(client) = test_client(&database) else {
            eprintln!("skipped: STREAMLING_BLOCKCHAIN_TEST_CLICKHOUSE_URL is not set");
            return;
        };
        let config = clickhouse_config(&database, "events");
        assert!(
            apply_schema_with(&client, &config)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            apply_schema_with(&client, &config)
                .await
                .unwrap()
                .is_empty()
        );

        // At-least-once redelivery writes the same event twice; FINAL collapses it.
        client
            .insert_json_rows("events", &[event_row("1:0xaaa:0", 10)])
            .await
            .unwrap();
        client
            .insert_json_rows("events", &[event_row("1:0xaaa:0", 10)])
            .await
            .unwrap();
        let rows = client
            .query(
                "SELECT count() AS events, min(insert_time) > '2000-01-01' AS stamped FROM events FINAL WHERE is_deleted = 0",
                10,
            )
            .await
            .unwrap();
        assert_eq!(rows["rows"][0]["events"], 1);
        assert_eq!(rows["rows"][0]["stamped"], 1);

        let schema = client.schema().await.unwrap();
        let events = schema["objects"]
            .as_array()
            .unwrap()
            .iter()
            .find(|object| object["name"] == "events")
            .unwrap();
        assert_eq!(
            events["sorting_key"],
            "chain_id, contract_alias, event_name, block_number, event_id"
        );

        let truncated = client
            .query("SELECT number FROM numbers(100000)", 3)
            .await
            .unwrap();
        assert_eq!(truncated["rows"].as_array().unwrap().len(), 3);
        assert_eq!(truncated["truncated"], true);

        let write = client
            .query("CREATE TABLE denied (x UInt8) ENGINE = Memory", 1)
            .await;
        assert!(write.unwrap_err().to_string().contains("readonly"));

        client
            .execute("CREATE TABLE legacy (event_id String, block_timestamp UInt64, insert_time DateTime64(3), is_deleted UInt8) ENGINE = ReplacingMergeTree(insert_time, is_deleted) ORDER BY event_id")
            .await
            .unwrap();
        let warnings = apply_schema_with(&client, &clickhouse_config(&database, "legacy"))
            .await
            .unwrap();
        assert!(warnings[0].contains("is ordered by (event_id)"));

        client
            .execute(&format!("DROP DATABASE {database}"))
            .await
            .unwrap();
    }

    fn read(lines: &[&str], max_rows: usize) -> Value {
        let mut reader = RowReader::new(max_rows);
        let mut truncated = false;
        for line in lines {
            if !reader.push(line).unwrap() {
                truncated = true;
                break;
            }
        }
        reader.finish(truncated).unwrap()
    }

    #[test]
    fn decodes_rows_like_the_sqlite_surface() {
        let value = read(
            &[
                r#"["event_name","block_number","amount","price","maybe"]"#,
                r#"["LowCardinality(String)","UInt64","UInt256","Decimal(38, 18)","Nullable(UInt64)"]"#,
                r#"["Transfer","18446744073709551615",115792089237316195423570985008687907853269984665640564039457584007913129639935,1.5,null]"#,
            ],
            10,
        );
        assert_eq!(value["columns"][0], "event_name");
        let row = &value["rows"][0];
        assert_eq!(row["event_name"], "Transfer");
        assert_eq!(row["block_number"], json!(u64::MAX));
        assert_eq!(
            row["amount"],
            "115792089237316195423570985008687907853269984665640564039457584007913129639935"
        );
        assert_eq!(row["price"], "1.5");
        assert_eq!(row["maybe"], Value::Null);
        assert_eq!(value["truncated"], false);
    }

    #[test]
    fn stops_after_max_rows() {
        let value = read(&[r#"["n"]"#, r#"["UInt64"]"#, "[1]", "[2]", "[3]"], 2);
        assert_eq!(value["rows"].as_array().unwrap().len(), 2);
        assert_eq!(value["truncated"], true);
        let exact = read(&[r#"["n"]"#, r#"["UInt64"]"#, "[1]", "[2]"], 2);
        assert_eq!(exact["truncated"], false);
    }

    #[test]
    fn splits_chunks_into_lines() {
        let mut buffer = LineBuffer::default();
        buffer.push(b"[\"a\"]\n[\"Str");
        assert_eq!(buffer.next_line().as_deref(), Some("[\"a\"]"));
        assert_eq!(buffer.next_line(), None);
        buffer.push(b"ing\"]\n[1]");
        assert_eq!(buffer.next_line().as_deref(), Some("[\"String\"]"));
        assert_eq!(buffer.rest().as_deref(), Some("[1]"));
    }

    #[test]
    fn quotes_string_literals() {
        assert_eq!(quote_string("it's"), "'it\\'s'");
        assert_eq!(quote_string("a\\b"), "'a\\\\b'");
    }
}
