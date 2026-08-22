use crate::{config::ProjectConfig, database, mcp, status as nest_status};
use anyhow::Result;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};
use tower_http::trace::TraceLayer;

#[derive(Clone)]
struct AppState {
    root: Arc<PathBuf>,
}

#[derive(Deserialize)]
struct SqlRequest {
    sql: String,
    max_rows: Option<usize>,
}

pub async fn serve(root: PathBuf, bind: String) -> Result<()> {
    let state = AppState {
        root: Arc::new(root),
    };
    let app = Router::new()
        .route("/", get(index))
        .route("/api/status", get(status))
        .route("/api/schema", get(schema))
        .route("/api/sql", post(sql))
        .route("/mcp", post(mcp_http))
        .layer(TraceLayer::new_for_http())
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    eprintln!("Streamling Blockchain Studio page and MCP listening on http://{bind}");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../../../web/index.html"))
}

async fn status(State(state): State<AppState>) -> impl IntoResponse {
    let result = nest_status::live(&state.root)
        .await
        .or_else(|_| nest_status::cached(&state.root));
    api_result(result)
}

async fn schema(State(state): State<AppState>) -> impl IntoResponse {
    api_result((|| {
        let config = ProjectConfig::load(&state.root)?;
        let conn = database::open(&config.absolute_database(&state.root))?;
        let semantics =
            std::fs::read_to_string(state.root.join("semantic.toml")).unwrap_or_default();
        Ok(json!({"schema": database::schema(&conn)?, "semantics": semantics}))
    })())
}

async fn sql(State(state): State<AppState>, Json(request): Json<SqlRequest>) -> impl IntoResponse {
    api_result((|| {
        mcp::ensure_read_only(&request.sql)?;
        let config = ProjectConfig::load(&state.root)?;
        let conn = database::open(&config.absolute_database(&state.root))?;
        database::query(
            &conn,
            &request.sql,
            request.max_rows.unwrap_or(200).min(1000),
        )
    })())
}

async fn mcp_http(State(state): State<AppState>, Json(request): Json<Value>) -> Json<Value> {
    Json(mcp::dispatch(&state.root, request))
}

fn api_result(result: Result<Value>) -> axum::response::Response {
    match result {
        Ok(value) => (StatusCode::OK, Json(value)).into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": error.to_string()})),
        )
            .into_response(),
    }
}
