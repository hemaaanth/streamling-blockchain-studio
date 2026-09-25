//! `publish`: build a demo's Evidence site from the project's SQLite data, stamp it with a
//! release manifest, check that it is safe to publish, and ship it to a directory or here.now.

use crate::{
    config::ProjectConfig,
    database,
    output::{CodedError, ErrorCode, Output},
    rpc, status,
};
use anyhow::{Context, Result, bail};
use reqwest::{RequestBuilder, StatusCode, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::process::Command;

/// Overrides the here.now API origin; tests point it at a local mock.
const HERENOW_API_ENV: &str = "STREAMLING_BLOCKCHAIN_HERENOW_API";
const HERENOW_API: &str = "https://here.now";
const HERENOW_KEY_ENV: &str = "HERENOW_API_KEY";
const HERENOW_KEY_HINT: &str = "create an API key at https://here.now, then export HERENOW_API_KEY=<key> and re-run; or pass --anonymous for a site that expires after 24 hours";
// here.now limits per site version.
const MAX_FILES: usize = 2_500;
const MAX_TOTAL_BYTES: u64 = 10_000_000_000;
const MAX_FILE_BYTES: u64 = 5_000_000_000;
const MAX_ANONYMOUS_FILE_BYTES: u64 = 250_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum Target {
    Dir,
    Herenow,
}

#[derive(clap::Args)]
pub struct PublishArgs {
    /// Evidence demo directory with a `sources:sqlite` npm script.
    demo: PathBuf,
    #[arg(long, value_enum)]
    target: Target,
    /// Directory that receives the site with `--target dir`.
    #[arg(long, required_if_eq("target", "dir"))]
    out: Option<PathBuf>,
    /// here.now display name.
    #[arg(long)]
    name: Option<String>,
    /// Publish to here.now without an API key; the site expires after 24 hours.
    #[arg(long)]
    anonymous: bool,
    /// Publish to here.now. Without it, `--target herenow` stops after the checks.
    #[arg(long)]
    yes: bool,
    /// Create a new here.now site instead of updating the one recorded for this demo.
    #[arg(long)]
    new_site: bool,
}

#[derive(Clone, Debug, Serialize)]
struct BuildFile {
    path: String,
    bytes: u64,
    sha256: String,
}

/// The here.now site a demo was last published to, so the next publish keeps its URL.
#[derive(Debug, Serialize, Deserialize)]
struct SiteRecord {
    slug: String,
    site_url: String,
    current_version_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    claim_token: Option<String>,
}

pub async fn run(root: &Path, out: &mut Output, args: PublishArgs) -> Result<Value> {
    let config = ProjectConfig::load(root)?;
    let demo = std::fs::canonicalize(&args.demo)
        .with_context(|| format!("open demo {}", args.demo.display()))?;
    let build = build_site(root, &config, &demo, out).await?;
    out.line(format!("✓ built {}", build.display()));

    let mut files = list_files(&build)?;
    files.retain(|file| file.path != "release.json");
    let manifest_path = build.join("release.json");
    std::fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest(root, &config, &files)?)?,
    )?;
    files.push(build_file(&build, "release.json".into())?);
    out.line(format!("✓ manifest written: {}", manifest_path.display()));

    scan_secrets(&build, &files, &secrets(&config))?;
    out.line("✓ checks passed: no RPC URL or credential found in the build");
    let total_bytes: u64 = files.iter().map(|file| file.bytes).sum();
    let mut largest = files.clone();
    largest.sort_by(|a, b| b.bytes.cmp(&a.bytes).then_with(|| a.path.cmp(&b.path)));
    largest.truncate(5);
    out.line(format!(
        "✓ size: {} in {} files; largest: {}",
        megabytes(total_bytes),
        files.len(),
        largest
            .iter()
            .map(|file| format!("{} ({})", file.path, megabytes(file.bytes)))
            .collect::<Vec<_>>()
            .join(", ")
    ));

    let mut data = json!({
        "target": args.target,
        "dry_run": false,
        "manifest_path": manifest_path,
        "total_bytes": total_bytes,
        "file_count": files.len(),
        "largest_files": largest
            .iter()
            .map(|file| json!({"path": file.path, "bytes": file.bytes}))
            .collect::<Vec<_>>(),
    });
    match args.target {
        Target::Dir => {
            let dest = args.out.context("--target dir requires --out")?;
            copy_site(&build, &files, &dest)?;
            out.line(format!("✓ copied to {}", dest.display()));
            data["out"] = json!(dest);
        }
        Target::Herenow => {
            check_limits(&files, args.anonymous)?;
            let name = demo.file_name().context("demo directory has no name")?;
            let record_path = root
                .join(".streamling-blockchain/publish")
                .join(format!("{}.json", name.to_string_lossy()));
            if !args.yes {
                let existing = read_record(&record_path)?.filter(|_| !args.new_site);
                let action = if existing.is_some() {
                    "update"
                } else {
                    "create"
                };
                out.line(format!(
                    "dry run: would {action} a public here.now site with {} files ({}); re-run with --yes to publish",
                    files.len(),
                    megabytes(total_bytes)
                ));
                data["dry_run"] = true.into();
                data["action"] = action.into();
                data["url"] = json!(existing.map(|site| site.site_url));
                return Ok(data);
            }
            let key = herenow_key(std::env::var(HERENOW_KEY_ENV).ok(), args.anonymous)?;
            let api = std::env::var(HERENOW_API_ENV).unwrap_or_else(|_| HERENOW_API.into());
            let site = herenow_publish(
                &api,
                key.as_deref(),
                &build,
                &files,
                &record_path,
                args.new_site,
                args.name.as_deref(),
                out,
            )
            .await?;
            out.line(format!(
                "✓ published: {}",
                site["url"].as_str().unwrap_or("")
            ));
            if let Some(expires_at) = site["expires_at"].as_str() {
                out.line(format!(
                    "✓ anonymous site expires at {expires_at} (24 hours after publishing)"
                ));
            }
            if let Some(claim_url) = site["claim_url"].as_str() {
                out.line(format!("✓ claim it to keep it: {claim_url}"));
            }
            for field in ["url", "slug", "expires_at", "claim_url"] {
                data[field] = site[field].clone();
            }
        }
    }
    Ok(data)
}

async fn build_site(
    root: &Path,
    config: &ProjectConfig,
    demo: &Path,
    out: &Output,
) -> Result<PathBuf> {
    if !config.sinks.sqlite {
        bail!(CodedError::new(
            ErrorCode::Validation,
            "publish builds from the SQLite sink, and this project writes only to ClickHouse; ClickHouse-backed demos are not supported yet"
        ))
    }
    let database = config.absolute_database(root);
    if !database.exists() {
        bail!(
            CodedError::new(
                ErrorCode::NotFound,
                format!(
                    "project database does not exist yet: {}",
                    database.display()
                )
            )
            .next(format!(
                "streamling-blockchain --project {} dev --exit-when-caught-up",
                root.display()
            ))
        )
    }
    let package_path = demo.join("package.json");
    let package: Value = serde_json::from_slice(
        &std::fs::read(&package_path)
            .with_context(|| format!("read {}", package_path.display()))?,
    )
    .with_context(|| format!("parse {}", package_path.display()))?;
    if package.pointer("/scripts/sources:sqlite").is_none() {
        bail!(CodedError::new(
            ErrorCode::Validation,
            format!(
                "{} has no `sources:sqlite` script; publish supports SQLite-backed Evidence demos only",
                package_path.display()
            )
        ))
    }
    if !demo.join("node_modules").exists() {
        npm(demo, &["ci"], &database, out).await?;
    }
    npm(demo, &["run", "sources:sqlite"], &database, out).await?;
    npm(demo, &["run", "build"], &database, out).await?;
    let build = demo.join("build");
    if !build.join("data/manifest.json").exists() {
        bail!(CodedError::new(
            ErrorCode::Validation,
            format!(
                "{} has no data/manifest.json, so the site would show only \"Timeout while initializing database\"; check the `sources:sqlite` output",
                build.display()
            )
        ))
    }
    Ok(build)
}

async fn npm(demo: &Path, args: &[&str], database: &Path, out: &Output) -> Result<()> {
    let status = Command::new("npm")
        .args(args)
        .current_dir(demo)
        .env("STREAMLING_BLOCKCHAIN_DB", database)
        .stdout(out.child_stdout())
        .status()
        .await
        .with_context(|| format!("run npm {}", args.join(" ")))?;
    if !status.success() {
        bail!("npm {} exited with {status}", args.join(" "))
    }
    Ok(())
}

/// Every file under `build`, sorted by its `/`-separated relative path.
fn list_files(build: &Path) -> Result<Vec<BuildFile>> {
    let mut paths = Vec::new();
    let mut dirs = vec![build.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).with_context(|| format!("read {}", dir.display()))? {
            let path = entry?.path();
            if path.is_dir() {
                dirs.push(path);
            } else {
                let relative = path.strip_prefix(build)?.components();
                paths.push(
                    relative
                        .map(|part| part.as_os_str().to_string_lossy())
                        .collect::<Vec<_>>()
                        .join("/"),
                );
            }
        }
    }
    paths.sort();
    paths
        .into_iter()
        .map(|path| build_file(build, path))
        .collect()
}

fn build_file(build: &Path, path: String) -> Result<BuildFile> {
    let bytes = std::fs::read(build.join(&path)).with_context(|| format!("read build/{path}"))?;
    Ok(BuildFile {
        path,
        bytes: bytes.len() as u64,
        sha256: hex::encode(Sha256::digest(&bytes)),
    })
}

/// `release.json`: what data the site was built from. Never the RPC URL or credentials.
fn manifest(root: &Path, config: &ProjectConfig, files: &[BuildFile]) -> Result<Value> {
    let progress = status::read_progress(root)?;
    let conn = database::open(&config.absolute_database(root))?;
    let mut statement = conn.prepare(
        "SELECT contract_alias, event_name, count(*) FROM events GROUP BY contract_alias, event_name ORDER BY contract_alias, event_name",
    )?;
    let event_counts = statement
        .query_map([], |row| {
            Ok(json!({
                "contract_alias": row.get::<_, String>(0)?,
                "event_name": row.get::<_, String>(1)?,
                "count": row.get::<_, i64>(2)?,
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(json!({
        "schema_version": 1,
        "cli_version": env!("CARGO_PKG_VERSION"),
        "built_at": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "chain": config.chain,
        "chain_id": config.chain_id,
        "contracts": config
            .contracts
            .iter()
            .map(|c| json!({"alias": c.alias, "address": c.address}))
            .collect::<Vec<_>>(),
        "discovery_rules": config
            .discovery_rules
            .iter()
            .map(|r| json!({"parent": r.parent_contract, "event": r.discovery_event, "child": r.child_contract}))
            .collect::<Vec<_>>(),
        "start_block": config.start_block,
        "end_block": config.end_block,
        "indexed_through": progress.as_ref().map(|p| p.indexed_through),
        "safe_head": progress.as_ref().map(|p| p.safe_head),
        "event_counts": event_counts,
        "files": files,
    }))
}

/// Values that must not appear in a published site, each with a label for the error.
fn secrets(config: &ProjectConfig) -> Vec<(&'static str, String)> {
    let mut urls = vec![config.rpc_url.clone()];
    if let Some(name) = &config.rpc_url_env {
        urls.extend(std::env::var(name).ok());
    }
    let mut secrets = Vec::new();
    for url in urls {
        if let Ok(parsed) = Url::parse(&url) {
            // Providers such as Alchemy put the key in the path; others in the query.
            if let Some(host) = parsed.host_str()
                && parsed.path().len() > 1
            {
                secrets.push(("part of the RPC URL", format!("{host}{}", parsed.path())));
            }
            secrets.extend(
                parsed
                    .query_pairs()
                    .map(|(_, value)| ("part of the RPC URL", value.into_owned()))
                    .filter(|(_, value)| value.len() >= 8),
            );
        }
        secrets.push(("the RPC URL", url));
    }
    secrets.extend(
        std::env::var("STREAMLING__CLICKHOUSE_SINK__PASSWORD")
            .ok()
            .map(|password| ("the ClickHouse password", password)),
    );
    secrets.retain(|(_, value)| !value.is_empty());
    secrets
}

fn scan_secrets(build: &Path, files: &[BuildFile], secrets: &[(&str, String)]) -> Result<()> {
    for file in files {
        let bytes = std::fs::read(build.join(&file.path))?;
        for (label, secret) in secrets {
            if bytes
                .windows(secret.len())
                .any(|window| window == secret.as_bytes())
            {
                bail!(CodedError::new(
                    ErrorCode::Validation,
                    format!(
                        "build/{} contains {label}; nothing was published",
                        file.path
                    )
                ))
            }
        }
    }
    Ok(())
}

fn check_limits(files: &[BuildFile], anonymous: bool) -> Result<()> {
    let total: u64 = files.iter().map(|file| file.bytes).sum();
    let max_file = if anonymous {
        MAX_ANONYMOUS_FILE_BYTES
    } else {
        MAX_FILE_BYTES
    };
    let problem = if files.len() > MAX_FILES {
        format!(
            "{} files exceed here.now's limit of {MAX_FILES} per site",
            files.len()
        )
    } else if total > MAX_TOTAL_BYTES {
        format!(
            "{} exceeds here.now's limit of {} per site",
            megabytes(total),
            megabytes(MAX_TOTAL_BYTES)
        )
    } else if let Some(file) = files.iter().find(|file| file.bytes > max_file) {
        format!(
            "build/{} is {}, over here.now's per-file limit of {}{}",
            file.path,
            megabytes(file.bytes),
            megabytes(max_file),
            if anonymous {
                " for anonymous sites"
            } else {
                ""
            }
        )
    } else {
        return Ok(());
    };
    bail!(CodedError::new(
        ErrorCode::Validation,
        format!("{problem}; aggregate the demo's source queries so the site ships less data")
    ))
}

fn copy_site(build: &Path, files: &[BuildFile], dest: &Path) -> Result<()> {
    if dest.exists() && std::fs::read_dir(dest)?.next().is_some() {
        if !dest.join("release.json").exists() || build.starts_with(dest.canonicalize()?) {
            bail!(CodedError::new(
                ErrorCode::Validation,
                format!(
                    "{} is not empty and holds no previous release; choose an empty directory",
                    dest.display()
                )
            ))
        }
        std::fs::remove_dir_all(dest)?;
    }
    for file in files {
        let target = dest.join(&file.path);
        std::fs::create_dir_all(target.parent().unwrap())?;
        std::fs::copy(build.join(&file.path), &target)
            .with_context(|| format!("copy {}", target.display()))?;
    }
    Ok(())
}

fn content_type(path: &str) -> &'static str {
    let extension = path
        .rsplit_once('.')
        .map(|(_, ext)| ext.to_ascii_lowercase());
    match extension.as_deref() {
        Some("wasm") => "application/wasm",
        Some("js" | "mjs") => "text/javascript",
        Some("json") => "application/json",
        Some("html") => "text/html",
        Some("css") => "text/css",
        Some("parquet") => "application/vnd.apache.parquet",
        Some("woff2") => "font/woff2",
        Some("woff") => "font/woff",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        Some("webmanifest") => "application/manifest+json",
        _ => "application/octet-stream",
    }
}

fn megabytes(bytes: u64) -> String {
    format!("{:.1} MB", bytes as f64 / 1e6)
}

/// The API key to send, if any: `--anonymous` sends none; otherwise one is required.
fn herenow_key(value: Option<String>, anonymous: bool) -> Result<Option<String>> {
    if anonymous {
        return Ok(None);
    }
    match value.filter(|key| !key.is_empty()) {
        Some(key) => Ok(Some(key)),
        None => bail!(
            CodedError::new(
                ErrorCode::MissingCredentials,
                format!("{HERENOW_KEY_ENV} is not set: {HERENOW_KEY_HINT}")
            )
            .next(HERENOW_KEY_HINT)
        ),
    }
}

fn read_record(path: &Path) -> Result<Option<SiteRecord>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parse {}", path.display()))
        .map(Some)
}

/// Creates or updates the here.now site, uploads the files it does not already store,
/// finalizes the version, and saves the site record for the next publish.
#[allow(clippy::too_many_arguments)]
async fn herenow_publish(
    api: &str,
    key: Option<&str>,
    build: &Path,
    files: &[BuildFile],
    record_path: &Path,
    new_site: bool,
    name: Option<&str>,
    out: &mut Output,
) -> Result<Value> {
    let api = Url::parse(api).with_context(|| format!("parse {HERENOW_API_ENV}"))?;
    let previous = read_record(record_path)?.filter(|_| !new_site);
    let client = rpc::client()?;
    let with_auth = |request: RequestBuilder| {
        let request = request.header("X-HereNow-Client", "streamling-blockchain/cli");
        match key {
            Some(key) => request.bearer_auth(key),
            None => request,
        }
    };

    let mut body = json!({
        "files": files
            .iter()
            .map(|file| json!({
                "path": file.path,
                "size": file.bytes,
                "contentType": content_type(&file.path),
                "hash": file.sha256,
            }))
            .collect::<Vec<_>>(),
    });
    if let Some(name) = name {
        body["displayName"] = name.into();
    }
    let request = match &previous {
        Some(site) => {
            body["baseVersionId"] = site.current_version_id.clone().into();
            if let Some(token) = &site.claim_token {
                body["claimToken"] = token.clone().into();
            }
            client.put(api.join(&format!("api/v1/publish/{}", site.slug))?)
        }
        None => client.post(api.join("api/v1/publish")?),
    };
    let created = send(with_auth(request).json(&body), "publish").await?;
    let upload = &created["upload"];
    let version_id = upload["versionId"]
        .as_str()
        .context("here.now publish response has no upload.versionId")?;

    // Presigned storage URLs: send exactly the given headers and no API key.
    for target in upload["uploads"].as_array().into_iter().flatten() {
        let path = target["path"].as_str().unwrap_or_default();
        let file = files
            .iter()
            .find(|file| file.path == path)
            .with_context(|| format!("here.now asked for a file not in the build: {path}"))?;
        let method =
            reqwest::Method::from_bytes(target["method"].as_str().unwrap_or("PUT").as_bytes())?;
        let url = target["url"]
            .as_str()
            .with_context(|| format!("here.now gave no upload URL for {path}"))?;
        let mut request = client
            .request(method, url)
            .body(std::fs::read(build.join(&file.path))?);
        for (header, value) in target["headers"].as_object().into_iter().flatten() {
            request = request.header(header, value.as_str().unwrap_or_default());
        }
        send(request, &format!("upload of {path}")).await?;
    }

    let finalize_url = upload["finalizeUrl"]
        .as_str()
        .context("here.now publish response has no upload.finalizeUrl")?;
    let finalized = send(
        with_auth(client.post(api.join(finalize_url)?)).json(&json!({"versionId": version_id})),
        "finalize",
    )
    .await?;
    for warning in finalized["warnings"].as_array().into_iter().flatten() {
        out.warn(format!(
            "here.now: {}",
            warning
                .as_str()
                .map_or_else(|| warning.to_string(), str::to_owned)
        ));
    }

    let slug = created["slug"]
        .as_str()
        .or(previous.as_ref().map(|site| site.slug.as_str()))
        .context("here.now publish response has no slug")?;
    let site_url = finalized["siteUrl"]
        .as_str()
        .or(created["siteUrl"].as_str())
        .context("here.now finalize response has no siteUrl")?;
    let record = SiteRecord {
        slug: slug.to_owned(),
        site_url: site_url.to_owned(),
        current_version_id: version_id.to_owned(),
        claim_token: created["claimToken"]
            .as_str()
            .map(str::to_owned)
            .or_else(|| previous.and_then(|site| site.claim_token)),
    };
    // Holds the claim token of an anonymous site.
    std::fs::create_dir_all(record_path.parent().unwrap())?;
    std::fs::write(record_path, serde_json::to_vec_pretty(&record)?)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(record_path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(json!({
        "url": record.site_url,
        "slug": record.slug,
        "expires_at": created["expiresAt"],
        "claim_url": created["claimUrl"],
    }))
}

/// Sends a here.now or storage request and maps HTTP failures to error codes.
async fn send(request: RequestBuilder, what: &str) -> Result<Value> {
    let response = request
        .send()
        .await
        .with_context(|| format!("here.now {what}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .with_context(|| format!("read here.now {what} response"))?;
    let body: Value = serde_json::from_str(&text).unwrap_or_default();
    if status.is_success() {
        return Ok(body);
    }
    let detail = match (body["code"].as_str(), body["message"].as_str()) {
        (Some(code), Some(message)) => format!("{code}: {message}"),
        _ => text.chars().take(300).collect(),
    };
    let message = format!("here.now {what} returned HTTP {status}: {detail}");
    let error = if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        CodedError::new(ErrorCode::TransientDependency, message)
    } else if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        CodedError::new(ErrorCode::MissingCredentials, message).next(HERENOW_KEY_HINT)
    } else if body["code"] == "version_conflict" {
        CodedError::new(ErrorCode::Validation, message)
            .next("re-run publish, or pass --new-site to create a new site")
    } else {
        CodedError::new(ErrorCode::Validation, message)
    };
    Err(error.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::classify;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    const RPC: &str = "https://rpc.example.invalid/v2/sekret-key-0123";

    /// A project with an events database and progress file, as `dev` leaves them.
    fn project(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "streamling-blockchain-publish-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join(".streamling-blockchain")).unwrap();
        write(
            &root,
            "streamling-blockchain.toml",
            &format!(
                "chain = \"ethereum\"\nchain_id = 1\nrpc_url = \"{RPC}\"\ndatabase = \".streamling-blockchain/events.db\"\nstart_block = 10\nend_block = 20\n\n[[contracts]]\nalias = \"token\"\naddress = \"0x1111111111111111111111111111111111111111\"\nabi = \"abis/token.json\"\n"
            ),
        );
        write(
            &root,
            "backfill-progress.json",
            r#"{"indexed_through":20,"observed_head":40,"safe_head":28,"confirmations":12,"caught_up":true,"updated_at_unix":1}"#,
        );
        rusqlite::Connection::open(root.join(".streamling-blockchain/events.db"))
            .unwrap()
            .execute_batch(
                "CREATE TABLE events (contract_alias TEXT, event_name TEXT);
                 INSERT INTO events VALUES ('token', 'Transfer'), ('token', 'Transfer'), ('token', 'Approval');",
            )
            .unwrap();
        root
    }

    fn write(dir: &Path, path: &str, contents: &str) {
        let path = dir.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, contents).unwrap();
    }

    fn sha256(contents: &str) -> String {
        hex::encode(Sha256::digest(contents))
    }

    #[test]
    fn manifest_records_data_and_files_without_the_rpc_url() {
        let root = project("manifest");
        let build = root.join("build");
        write(&build, "index.html", "<html>");
        write(&build, "data/manifest.json", "{}");
        let files = list_files(&build).unwrap();
        let value = manifest(&root, &ProjectConfig::load(&root).unwrap(), &files).unwrap();
        assert_eq!(
            value["event_counts"],
            json!([
                {"contract_alias": "token", "event_name": "Approval", "count": 1},
                {"contract_alias": "token", "event_name": "Transfer", "count": 2},
            ])
        );
        assert_eq!(
            value["files"],
            json!([
                {"path": "data/manifest.json", "bytes": 2, "sha256": sha256("{}")},
                {"path": "index.html", "bytes": 6, "sha256": sha256("<html>")},
            ])
        );
        assert_eq!(
            value["contracts"],
            json!([{"alias": "token", "address": "0x1111111111111111111111111111111111111111"}])
        );
        assert_eq!(
            (&value["start_block"], &value["end_block"]),
            (&json!(10), &json!(20))
        );
        assert_eq!(
            (&value["indexed_through"], &value["safe_head"]),
            (&json!(20), &json!(28))
        );
        assert!(value["built_at"].as_str().unwrap().ends_with('Z'));
        assert!(!value.to_string().contains("rpc.example.invalid"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn secret_scan_finds_the_rpc_url_and_its_key() {
        let root = project("secrets");
        let mut config = ProjectConfig::load(&root).unwrap();
        let build = root.join("build");
        write(&build, "a.js", "fetch('/data/manifest.json')");
        write(
            &build,
            "b.js",
            "const rpc = 'wss://rpc.example.invalid/v2/sekret-key-0123';",
        );
        let files = list_files(&build).unwrap();
        assert!(scan_secrets(&build, &files[..1], &secrets(&config)).is_ok());
        let error = scan_secrets(&build, &files, &secrets(&config)).unwrap_err();
        assert_eq!(classify(&error).0, ErrorCode::Validation);
        assert_eq!(
            error.to_string(),
            "build/b.js contains part of the RPC URL; nothing was published"
        );

        config.rpc_url = "https://rpc.example.invalid/?apikey=abcdef123456".into();
        write(&build, "a.js", "key=abcdef123456");
        assert!(scan_secrets(&build, &files[..1], &secrets(&config)).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn limits_and_content_types() {
        let file = |path: &str, bytes| BuildFile {
            path: path.into(),
            bytes,
            sha256: String::new(),
        };
        assert!(check_limits(&[file("a.wasm", 300_000_000)], false).is_ok());
        let error = check_limits(&[file("a.wasm", 300_000_000)], true).unwrap_err();
        assert_eq!(classify(&error).0, ErrorCode::Validation);
        assert!(error.to_string().contains("for anonymous sites"));
        let many = (0..2_501)
            .map(|i| file(&format!("{i}.js"), 1))
            .collect::<Vec<_>>();
        assert!(check_limits(&many, false).is_err());
        assert!(
            check_limits(&[file("a", 6_000_000_000), file("b", 6_000_000_000)], false).is_err()
        );

        assert_eq!(content_type("_app/duckdb-eh.wasm"), "application/wasm");
        assert_eq!(content_type("_app/entry.MJS"), "text/javascript");
        assert_eq!(
            content_type("data/a/b.parquet"),
            "application/vnd.apache.parquet"
        );
        assert_eq!(content_type("fonts/x.woff2"), "font/woff2");
        assert_eq!(content_type("LICENSE"), "application/octet-stream");
    }

    #[test]
    fn herenow_key_is_required_unless_anonymous() {
        assert_eq!(herenow_key(None, true).unwrap(), None);
        assert_eq!(
            herenow_key(Some("k".into()), false).unwrap().as_deref(),
            Some("k")
        );
        let (code, next) = classify(&herenow_key(Some(String::new()), false).unwrap_err());
        assert_eq!(code, ErrorCode::MissingCredentials);
        assert!(next.unwrap().contains("https://here.now"));
    }

    struct Seen {
        method: String,
        path: String,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    }

    impl Seen {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.as_str())
        }

        fn json(&self) -> Value {
            serde_json::from_slice(&self.body).unwrap()
        }
    }

    /// A here.now stand-in: `data/manifest.json` is always already stored.
    async fn mock_herenow() -> (String, Arc<Mutex<Vec<Seen>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let (base, log) = (origin.clone(), seen.clone());
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(&mut socket);
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                let mut words = line.split_whitespace();
                let method = words.next().unwrap().to_owned();
                let path = words.next().unwrap().to_owned();
                let mut headers = Vec::new();
                loop {
                    line.clear();
                    reader.read_line(&mut line).await.unwrap();
                    let Some((key, value)) = line.trim_end().split_once(':') else {
                        break;
                    };
                    headers.push((key.to_ascii_lowercase(), value.trim().to_owned()));
                }
                let length = headers
                    .iter()
                    .find(|(key, _)| key == "content-length")
                    .map_or(0, |(_, value)| value.parse().unwrap());
                let mut body = vec![0; length];
                reader.read_exact(&mut body).await.unwrap();
                let request = Seen {
                    method,
                    path,
                    headers,
                    body,
                };
                let reply = herenow_reply(&base, &request).to_string();
                log.lock().unwrap().push(request);
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{reply}",
                            reply.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        });
        (origin, seen)
    }

    fn herenow_reply(origin: &str, request: &Seen) -> Value {
        match (request.method.as_str(), request.path.as_str()) {
            ("POST", "/api/v1/publish") | ("PUT", "/api/v1/publish/calm-otter") => {
                let create = request.method == "POST";
                let (skipped, uploads): (Vec<String>, Vec<String>) = request.json()["files"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|file| file["path"].as_str().unwrap().to_owned())
                    .partition(|path| path == "data/manifest.json");
                let mut reply = json!({
                    "slug": "calm-otter",
                    "siteUrl": "https://calm-otter.here.now/",
                    "upload": {
                        "versionId": if create { "v1" } else { "v2" },
                        "uploads": uploads.iter().map(|path| json!({
                            "path": path,
                            "method": "PUT",
                            "url": format!("{origin}/r2/{path}?sig=abc"),
                            "headers": {"Content-Type": content_type(path), "x-amz-meta-check": "presigned"},
                        })).collect::<Vec<_>>(),
                        "skipped": skipped,
                        "finalizeUrl": format!("{origin}/api/v1/publish/calm-otter/finalize"),
                        "expiresInSeconds": 3600,
                    },
                });
                if create && request.header("authorization").is_none() {
                    reply["claimToken"] = "claim-123".into();
                    reply["claimUrl"] = "https://here.now/c/claim-123".into();
                    reply["expiresAt"] = "2026-09-26T12:00:00Z".into();
                }
                reply
            }
            ("PUT", _) => Value::Null,
            ("POST", "/api/v1/publish/calm-otter/finalize") => json!({
                "success": true,
                "slug": "calm-otter",
                "siteUrl": "https://calm-otter.here.now/",
                "warnings": ["proxy.json ignored"],
            }),
            other => panic!("unexpected request {other:?}"),
        }
    }

    #[tokio::test]
    async fn herenow_uploads_missing_files_and_updates_the_recorded_site() {
        let root = project("herenow");
        let build = root.join("build");
        write(&build, "index.html", "<html>");
        write(&build, "data/manifest.json", "{}");
        write(&build, "_app/duckdb.wasm", "\0asm");
        let files = list_files(&build).unwrap();
        let record_path = root.join(".streamling-blockchain/publish/demo.json");
        let (api, seen) = mock_herenow().await;
        let mut out = Output {
            json: true,
            warnings: Vec::new(),
        };

        // Anonymous create.
        let site = herenow_publish(
            &api,
            None,
            &build,
            &files,
            &record_path,
            false,
            Some("Demo"),
            &mut out,
        )
        .await
        .unwrap();
        assert_eq!(
            site,
            json!({"url": "https://calm-otter.here.now/", "slug": "calm-otter",
                   "expires_at": "2026-09-26T12:00:00Z", "claim_url": "https://here.now/c/claim-123"})
        );
        assert_eq!(out.warnings, ["here.now: proxy.json ignored"]);
        {
            let seen = seen.lock().unwrap();
            assert_eq!(seen.len(), 4);
            let create = &seen[0];
            assert_eq!(
                (create.method.as_str(), create.path.as_str()),
                ("POST", "/api/v1/publish")
            );
            assert_eq!(
                create.header("x-herenow-client"),
                Some("streamling-blockchain/cli")
            );
            assert_eq!(create.header("authorization"), None);
            assert_eq!(
                create.json(),
                json!({"displayName": "Demo", "files": [
                    {"path": "_app/duckdb.wasm", "size": 4, "contentType": "application/wasm", "hash": sha256("\0asm")},
                    {"path": "data/manifest.json", "size": 2, "contentType": "application/json", "hash": sha256("{}")},
                    {"path": "index.html", "size": 6, "contentType": "text/html", "hash": sha256("<html>")},
                ]})
            );
            for (upload, (path, body)) in seen[1..3]
                .iter()
                .zip([("_app/duckdb.wasm", "\0asm"), ("index.html", "<html>")])
            {
                assert_eq!(upload.method, "PUT");
                assert_eq!(upload.path, format!("/r2/{path}?sig=abc"));
                assert_eq!(upload.header("x-amz-meta-check"), Some("presigned"));
                assert_eq!(upload.header("content-type"), Some(content_type(path)));
                assert_eq!(upload.header("authorization"), None);
                assert_eq!(upload.body, body.as_bytes());
            }
            assert_eq!(seen[3].path, "/api/v1/publish/calm-otter/finalize");
            assert_eq!(seen[3].json(), json!({"versionId": "v1"}));
        }
        let record: Value = serde_json::from_slice(&std::fs::read(&record_path).unwrap()).unwrap();
        assert_eq!(
            record,
            json!({"slug": "calm-otter", "site_url": "https://calm-otter.here.now/",
                   "current_version_id": "v1", "claim_token": "claim-123"})
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&record_path)
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }

        // The next publish updates the same site with a key.
        let site = herenow_publish(
            &api,
            Some("test-key"),
            &build,
            &files,
            &record_path,
            false,
            None,
            &mut out,
        )
        .await
        .unwrap();
        assert_eq!(site["url"], "https://calm-otter.here.now/");
        assert_eq!(site["expires_at"], Value::Null);
        {
            let seen = seen.lock().unwrap();
            let update = &seen[4];
            assert_eq!(
                (update.method.as_str(), update.path.as_str()),
                ("PUT", "/api/v1/publish/calm-otter")
            );
            assert_eq!(update.header("authorization"), Some("Bearer test-key"));
            assert_eq!(update.json()["baseVersionId"], "v1");
            assert_eq!(update.json()["claimToken"], "claim-123");
            assert!(
                seen[5..7]
                    .iter()
                    .all(|upload| upload.header("authorization").is_none())
            );
            assert_eq!(seen[7].header("authorization"), Some("Bearer test-key"));
            assert_eq!(seen[7].json(), json!({"versionId": "v2"}));
        }
        let record: Value = serde_json::from_slice(&std::fs::read(&record_path).unwrap()).unwrap();
        assert_eq!(record["current_version_id"], "v2");
        assert_eq!(record["claim_token"], "claim-123");

        // --new-site ignores the record.
        herenow_publish(
            &api,
            Some("test-key"),
            &build,
            &files,
            &record_path,
            true,
            None,
            &mut out,
        )
        .await
        .unwrap();
        assert_eq!(seen.lock().unwrap()[8].path, "/api/v1/publish");
        std::fs::remove_dir_all(root).unwrap();
    }

    async fn publish_json(args: &[&Path]) -> Value {
        let args = ["streamling-blockchain", "--json", "publish"]
            .into_iter()
            .map(PathBuf::from)
            .chain(args.iter().map(|arg| arg.to_path_buf()));
        crate::execute(args).await.envelope()
    }

    #[tokio::test]
    async fn publish_refuses_projects_it_cannot_build() {
        let root = project("refuse");
        let demo = root.join("demo");
        write(&demo, "package.json", r#"{"scripts": {"build": "true"}}"#);
        let args = [
            Path::new("--project"),
            &root,
            &demo,
            Path::new("--target"),
            Path::new("herenow"),
        ];

        let value = publish_json(&args).await;
        assert_eq!(value["error"]["code"], "validation");
        assert!(
            value["error"]["message"]
                .as_str()
                .unwrap()
                .contains("sources:sqlite")
        );

        std::fs::remove_file(root.join(".streamling-blockchain/events.db")).unwrap();
        let value = publish_json(&args).await;
        assert_eq!(value["error"]["code"], "not_found");
        assert!(
            value["error"]["suggested_next"]
                .as_str()
                .unwrap()
                .ends_with("dev --exit-when-caught-up")
        );

        let config = root.join("streamling-blockchain.toml");
        let text = std::fs::read_to_string(&config).unwrap();
        std::fs::write(
            &config,
            format!("{text}\n[sinks]\nsqlite = false\n\n[sinks.clickhouse]\ntable = \"events\"\n"),
        )
        .unwrap();
        let value = publish_json(&args).await;
        assert_eq!(value["error"]["code"], "validation");
        assert!(
            value["error"]["message"]
                .as_str()
                .unwrap()
                .contains("ClickHouse")
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn publish_builds_the_demo_and_copies_it_to_a_directory() {
        if std::process::Command::new("npm")
            .arg("--version")
            .output()
            .is_err()
        {
            eprintln!("skipping: npm is not installed");
            return;
        }
        let root = project("dir");
        let demo = root.join("demo");
        write(
            &demo,
            "package.json",
            r#"{"name": "fake-demo", "private": true, "scripts": {
                "sources:sqlite": "test -f \"$STREAMLING_BLOCKCHAIN_DB\"",
                "build": "mkdir -p build/data && echo '{}' > build/data/manifest.json && echo '<html></html>' > build/index.html"
            }}"#,
        );
        std::fs::create_dir_all(demo.join("node_modules")).unwrap();
        let site = root.join("site");
        let args = [
            Path::new("--project"),
            &root,
            &demo,
            Path::new("--target"),
            Path::new("dir"),
            Path::new("--out"),
            &site,
        ];

        let value = publish_json(&args).await;
        assert_eq!(value["ok"], true, "{value}");
        assert_eq!(value["data"]["target"], "dir");
        assert_eq!(value["data"]["file_count"], 3);
        assert_eq!(value["data"]["out"], json!(site));
        let release: Value =
            serde_json::from_slice(&std::fs::read(site.join("release.json")).unwrap()).unwrap();
        assert_eq!(release["event_counts"].as_array().unwrap().len(), 2);
        assert_eq!(release["files"].as_array().unwrap().len(), 2);
        assert!(site.join("data/manifest.json").exists());
        // A previous release may be replaced.
        assert_eq!(publish_json(&args).await["ok"], true);

        let other = root.join("other");
        write(&other, "keep.txt", "mine");
        let mut args = args;
        args[6] = &other;
        let value = publish_json(&args).await;
        assert_eq!(value["error"]["code"], "validation");
        assert!(other.join("keep.txt").exists());

        let value = publish_json(&[
            Path::new("--project"),
            &root,
            &demo,
            Path::new("--target"),
            Path::new("herenow"),
        ])
        .await;
        assert_eq!(value["data"]["dry_run"], true, "{value}");
        assert_eq!(value["data"]["action"], "create");
        std::fs::remove_dir_all(root).unwrap();
    }
}
