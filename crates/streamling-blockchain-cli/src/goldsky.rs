use anyhow::{Context, Result, bail};
use std::path::Path;
use tokio::process::Command;

pub async fn ensure_rpc(cli: &Path, endpoint_name: &str, chain_id: u64) -> Result<String> {
    validate_endpoint_name(endpoint_name)?;
    ensure_edge_capability(cli).await?;

    let existing = run(cli, &["edge", "get", endpoint_name, "--color", "false"]).await?;
    if !existing.status.success() {
        let created = run(
            cli,
            &[
                "edge",
                "create",
                endpoint_name,
                "--product",
                "rpc",
                "--color",
                "false",
            ],
        )
        .await?;
        if !created.status.success() {
            bail!(
                "Goldsky could not create Edge endpoint '{}': {}",
                endpoint_name,
                command_error(&created)
            );
        }
    }

    let revealed = run(cli, &["edge", "reveal", endpoint_name, "--color", "false"]).await?;
    if !revealed.status.success() {
        bail!(
            "Goldsky could not reveal Edge endpoint '{}': {}",
            endpoint_name,
            command_error(&revealed)
        );
    }
    let revealed_stdout =
        String::from_utf8(revealed.stdout).context("Goldsky Edge key was not UTF-8")?;
    let key = revealed_stdout
        .lines()
        .rev()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .context("Goldsky Edge reveal returned no API key")?;
    if !key
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        bail!("Goldsky Edge reveal returned an invalid API key")
    }
    Ok(format!(
        "https://edge.goldsky.com/standard/evm/{chain_id}?key={key}"
    ))
}

async fn ensure_edge_capability(cli: &Path) -> Result<()> {
    let output = run(cli, &["edge", "--help"]).await?;
    let help = String::from_utf8_lossy(&output.stdout);
    if !output.status.success() || !help.contains("goldsky edge create") {
        bail!(
            "{} does not provide `goldsky edge`; install Goldsky CLI 13.9.0 or newer from https://docs.goldsky.com/installation",
            cli.display()
        )
    }
    Ok(())
}

async fn run(cli: &Path, arguments: &[&str]) -> Result<std::process::Output> {
    Command::new(cli)
        .args(arguments)
        .output()
        .await
        .with_context(|| format!("run {} {}", cli.display(), arguments.join(" ")))
}

fn command_error(output: &std::process::Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    stderr
        .lines()
        .rev()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("command failed without an error message")
        .to_owned()
}

fn validate_endpoint_name(name: &str) -> Result<()> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        bail!("Goldsky endpoint names may contain only letters, numbers, '-' and '_'")
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};

    #[tokio::test]
    async fn creates_and_reveals_edge_endpoint() {
        let root = std::env::temp_dir().join(format!(
            "streamling-blockchain-goldsky-{}",
            std::process::id()
        ));
        let cli = root.join("goldsky");
        fs::create_dir_all(&root).unwrap();
        fs::write(
            &cli,
            "#!/bin/sh\nif [ \"$1 $2\" = \"edge --help\" ]; then echo 'goldsky edge create'; exit 0; fi\nif [ \"$1 $2\" = \"edge get\" ]; then exit 1; fi\nif [ \"$1 $2\" = \"edge create\" ]; then exit 0; fi\nif [ \"$1 $2\" = \"edge reveal\" ]; then echo test_key_123; exit 0; fi\nexit 1\n",
        )
        .unwrap();
        fs::set_permissions(&cli, fs::Permissions::from_mode(0o700)).unwrap();

        let url = ensure_rpc(&PathBuf::from(&cli), "streamling-blockchain", 42)
            .await
            .unwrap();
        assert_eq!(
            url,
            "https://edge.goldsky.com/standard/evm/42?key=test_key_123"
        );
        fs::remove_dir_all(root).unwrap();
    }
}
