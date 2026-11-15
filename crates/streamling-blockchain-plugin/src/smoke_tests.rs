//! Runs the event source and SQLite sink against `tests/mock_rpc.py`, without the streamling runtime.
use crate::{sink::SqliteSink, source::EvmEventSource};
use abi_stable::external_types::crossbeam_channel;
use rusqlite::Connection;
use serde_json::json;
use std::{
    collections::HashMap,
    io::{BufRead, BufReader},
    path::Path,
    process::{Child, Command, Stdio},
};
use streamling_plugin::{
    PluginStateBackendConfig, SinkPlugin, SourcePlugin, api::PluginStateBackendFactory,
    r#async::DirectTokioProxy, ffi::PluginMetricsRecorder,
};

const PARENT: &str = "0x1111111111111111111111111111111111111111";

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start_mock_rpc(repo: &Path) -> (Server, String) {
    let mut server = Server(
        Command::new("python3")
            .arg(repo.join("tests/mock_rpc.py"))
            .arg("0")
            .stdout(Stdio::piped())
            .spawn()
            .expect("python3 runs tests/mock_rpc.py"),
    );
    // The server prints its port after it binds, so it accepts connections from here on.
    let mut port = String::new();
    BufReader::new(server.0.stdout.take().unwrap())
        .read_line(&mut port)
        .unwrap();
    let port = port.trim();
    assert!(!port.is_empty(), "mock RPC exited before it bound a port");
    (server, format!("http://127.0.0.1:{port}"))
}

fn state() -> PluginStateBackendFactory {
    PluginStateBackendFactory::new(PluginStateBackendConfig::new(
        "smoke".into(),
        "events".into(),
        json!({"backend_type": "InMemory", "postgres": null, "sqlite": null}).to_string(),
    ))
}

#[test]
fn mock_rpc_events_reach_sqlite() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let (_server, rpc_url) = start_mock_rpc(&repo);
    let db = std::env::temp_dir().join(format!("smoke-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&db);
    let abi = repo.join("tests/transfer_abi.json");
    let spec = json!({
        "contracts": [{"alias": "factory", "address": PARENT, "abi_path": abi}],
        "discovery_rules": [{
            "parent_contract": "factory",
            "discovery_event": "Transfer",
            "address_field": "to",
            "child_contract": "token",
            "child_abi_path": abi,
        }],
    });
    let options = |pairs: &[(&str, String)]| {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), value.clone()))
            .collect::<HashMap<_, _>>()
    };
    let (metrics, _receiver) = crossbeam_channel::unbounded();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let rt = || DirectTokioProxy::new().into_async_runtime_obj();
        let source = EvmEventSource::new(
            rt(),
            state(),
            PluginMetricsRecorder::new(metrics.clone()),
            options(&[
                ("rpc_url", rpc_url),
                ("chain_id", "1".into()),
                ("start_block", "100".into()),
                ("end_block", "100".into()),
                ("confirmations", "0".into()),
                ("spec", spec.to_string()),
            ]),
        )
        .unwrap();
        source.initialize().await.unwrap();
        let sink = SqliteSink::new(
            source.output_schema().unwrap(),
            rt(),
            state(),
            PluginMetricsRecorder::new(metrics.clone()),
            options(&[("db_path", db.display().to_string())]),
        )
        .unwrap();
        // The first batch covers block 100; the next finds nothing left before end_block.
        for _ in 0..2 {
            sink.process_batch(source.generate_batch().await.unwrap())
                .await
                .unwrap();
        }
    });

    let connection = Connection::open(&db).unwrap();
    let rows = connection
        .prepare(
            "SELECT contract_alias || ':' || event_name || ':' || address || ':' || json_extract(data, '$.value')
             FROM events ORDER BY log_index",
        )
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        rows,
        [
            format!("factory:Transfer:{PARENT}:1"),
            "token:Transfer:0x2222222222222222222222222222222222222222:2".to_owned(),
        ],
        "the parent event and the event of the child it discovered both reach SQLite; \
         the mock honors address filters, so the child event needs the child query"
    );
    let child = connection
        .query_row(
            "SELECT \"to\", value, block_timestamp FROM token__transfer",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        child,
        (
            "0x3333333333333333333333333333333333333333".into(),
            "2".into(),
            0x66
        )
    );
    let _ = std::fs::remove_file(&db);
}
