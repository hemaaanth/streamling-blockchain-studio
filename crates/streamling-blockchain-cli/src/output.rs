//! The `--json` envelope that every command prints, and the error codes it reports.

use serde::Serialize;
use serde_json::{Value, json};
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    Validation,
    NotFound,
    MissingCredentials,
    TransientDependency,
    Internal,
}

/// An error whose code is known where it happens. It prints exactly like the error it
/// wraps, so human error output does not change.
#[derive(Debug)]
pub struct CodedError {
    pub code: ErrorCode,
    pub suggested_next: Option<String>,
    inner: anyhow::Error,
}

impl CodedError {
    pub fn new<M>(code: ErrorCode, message: M) -> Self
    where
        M: fmt::Display + fmt::Debug + Send + Sync + 'static,
    {
        Self::wrap(code, anyhow::Error::msg(message))
    }

    pub fn wrap(code: ErrorCode, inner: anyhow::Error) -> Self {
        Self {
            code,
            suggested_next: None,
            inner,
        }
    }

    pub fn next(mut self, command: impl Into<String>) -> Self {
        self.suggested_next = Some(command.into());
        self
    }
}

impl fmt::Display for CodedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl std::error::Error for CodedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.inner.source()
    }
}

/// The outermost known error in the chain decides the code.
pub fn classify(error: &anyhow::Error) -> (ErrorCode, Option<String>) {
    for cause in error.chain() {
        if let Some(coded) = cause.downcast_ref::<CodedError>() {
            return (coded.code, coded.suggested_next.clone());
        }
        let code = if let Some(error) = cause.downcast_ref::<reqwest::Error>() {
            if error.is_builder() {
                ErrorCode::Validation
            } else {
                ErrorCode::TransientDependency
            }
        } else if cause.is::<std::env::VarError>() {
            ErrorCode::MissingCredentials
        } else if let Some(error) = cause.downcast_ref::<rusqlite::Error>() {
            match error.sqlite_error_code() {
                Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked) => {
                    ErrorCode::TransientDependency
                }
                _ => ErrorCode::Validation,
            }
        } else if cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
        {
            ErrorCode::NotFound
        } else if cause.is::<toml::de::Error>() {
            ErrorCode::Validation
        } else {
            continue;
        };
        return (code, None);
    }
    (ErrorCode::Internal, None)
}

pub fn envelope(command: &str, result: &anyhow::Result<Value>, warnings: &[String]) -> Value {
    let (data, error) = match result {
        Ok(data) => (data.clone(), Value::Null),
        Err(error) => {
            let (code, suggested_next) = classify(error);
            (
                Value::Null,
                json!({
                    "code": code,
                    "message": format!("{error:#}"),
                    "retryable": code == ErrorCode::TransientDependency,
                    "suggested_next": suggested_next,
                }),
            )
        }
    };
    json!({
        "schema_version": 1,
        "ok": result.is_ok(),
        "command": command,
        "data": data,
        "warnings": warnings,
        "error": error,
    })
}

/// Human progress lines and warnings. In `--json` mode stdout holds only the envelope.
pub struct Output {
    pub json: bool,
    pub warnings: Vec<String>,
}

impl Output {
    pub fn line(&self, text: impl fmt::Display) {
        if !self.json {
            println!("{text}");
        }
    }

    pub fn warn(&mut self, text: impl Into<String>) {
        let text = text.into();
        if self.json {
            self.warnings.push(text);
        } else {
            eprintln!("warning: {text}");
        }
    }

    /// Where child processes send stdout: stderr in `--json` mode.
    pub fn child_stdout(&self) -> std::process::Stdio {
        if self.json {
            std::io::stderr().into()
        } else {
            std::process::Stdio::inherit()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;

    #[test]
    fn success_envelope_carries_data() {
        let value = envelope("sql", &Ok(json!({"rows": []})), &["careful".into()]);
        assert_eq!(
            value,
            json!({"schema_version": 1, "ok": true, "command": "sql", "data": {"rows": []},
                   "warnings": ["careful"], "error": null})
        );
    }

    #[test]
    fn failure_envelope_carries_code_and_next_step() {
        let error = anyhow::Error::new(
            CodedError::new(ErrorCode::NotFound, "database does not exist yet")
                .next("streamling-blockchain dev"),
        )
        .context("open database");
        let value = envelope("clickhouse schema", &Err(error), &[]);
        assert_eq!(
            value,
            json!({"schema_version": 1, "ok": false, "command": "clickhouse schema", "data": null,
                   "warnings": [], "error": {"code": "not_found",
                   "message": "open database: database does not exist yet",
                   "retryable": false, "suggested_next": "streamling-blockchain dev"}})
        );
    }

    #[test]
    fn coded_error_prints_like_the_error_it_wraps() {
        let inner = || {
            Err::<(), _>(std::io::Error::other("disk full"))
                .context("write file")
                .unwrap_err()
        };
        let coded = anyhow::Error::new(CodedError::wrap(ErrorCode::Internal, inner()));
        assert_eq!(format!("{coded:?}"), format!("{:?}", inner()));
        assert_eq!(format!("{coded:#}"), "write file: disk full");
    }

    #[tokio::test]
    async fn classifies_reqwest_errors_as_transient() {
        // Port 9 on loopback refuses the connection without leaving the machine.
        let error = reqwest::Client::new()
            .get("http://127.0.0.1:9/")
            .send()
            .await
            .context("eth_chainId request")
            .unwrap_err();
        assert_eq!(classify(&error).0, ErrorCode::TransientDependency);
        let value = envelope("status", &Err(error), &[]);
        assert_eq!(value["error"]["retryable"], true);
    }

    #[test]
    fn classifies_std_errors() {
        let missing_env = std::env::var("STREAMLING_BLOCKCHAIN_SURELY_UNSET")
            .context("read $STREAMLING_BLOCKCHAIN_SURELY_UNSET")
            .unwrap_err();
        assert_eq!(classify(&missing_env).0, ErrorCode::MissingCredentials);
        let missing_file = std::fs::read("/nonexistent/streamling-blockchain.toml")
            .context("read config")
            .unwrap_err();
        assert_eq!(classify(&missing_file).0, ErrorCode::NotFound);
        let bad_sql = rusqlite::Connection::open_in_memory()
            .unwrap()
            .prepare("SELEC 1")
            .context("prepare SQL")
            .unwrap_err();
        assert_eq!(classify(&bad_sql).0, ErrorCode::Validation);
        assert_eq!(classify(&anyhow::anyhow!("boom")).0, ErrorCode::Internal);
    }
}
