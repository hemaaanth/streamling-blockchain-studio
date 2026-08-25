use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectConfig {
    pub chain: String,
    pub chain_id: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub rpc_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_url_env: Option<String>,
    pub database: PathBuf,
    pub start_block: u64,
    #[serde(default = "default_confirmations")]
    pub confirmations: u64,
    #[serde(default = "default_window")]
    pub window: u64,
    pub contracts: Vec<ContractConfig>,
    #[serde(default)]
    pub discovery_rules: Vec<DiscoveryRule>,
    #[serde(default)]
    pub sinks: SinkConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractConfig {
    pub alias: String,
    pub address: String,
    pub abi: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryRule {
    pub parent_contract: String,
    pub discovery_event: String,
    pub address_field: String,
    pub child_contract: String,
    pub child_abi: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkConfig {
    #[serde(default = "default_sqlite_sink")]
    pub sqlite: bool,
    #[serde(default)]
    pub clickhouse: Option<ClickHouseSinkConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickHouseSinkConfig {
    pub table: String,
    #[serde(default)]
    pub compression: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
}

fn default_confirmations() -> u64 {
    12
}
fn default_window() -> u64 {
    2_000
}
fn default_sqlite_sink() -> bool {
    true
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self {
            sqlite: true,
            clickhouse: None,
        }
    }
}

impl ProjectConfig {
    pub fn load(root: &Path) -> Result<Self> {
        let path = root.join("streamling-blockchain.toml");
        let text =
            std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        let config: Self =
            toml::from_str(&text).with_context(|| format!("parse {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn save(&self, root: &Path) -> Result<()> {
        self.validate()?;
        std::fs::create_dir_all(root)?;
        let path = root.join("streamling-blockchain.toml");
        std::fs::write(&path, toml::to_string_pretty(self)?)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    }

    pub fn rpc_url_value(&self) -> Result<String> {
        if let Some(name) = &self.rpc_url_env {
            return std::env::var(name).with_context(|| format!("read ${name}"));
        }
        Ok(self.rpc_url.clone())
    }

    pub fn validate(&self) -> Result<()> {
        if self.rpc_url.is_empty() == self.rpc_url_env.is_none() {
            bail!("exactly one of rpc_url or rpc_url_env is required")
        }
        if self.contracts.is_empty() {
            bail!("at least one contract is required")
        }
        if self.window == 0 {
            bail!("window must be greater than zero")
        }
        if !self.sinks.sqlite && self.sinks.clickhouse.is_none() {
            bail!("at least one sink is required")
        }
        if let Some(clickhouse) = &self.sinks.clickhouse {
            validate_alias(&clickhouse.table).context("ClickHouse table")?;
            if let Some(database) = &clickhouse.database {
                validate_alias(database).context("ClickHouse database")?;
            }
            if let Some(compression) = &clickhouse.compression {
                match compression.as_str() {
                    "none" | "gzip" | "zstd" | "lz4" => {}
                    _ => bail!("ClickHouse compression must be one of none, gzip, zstd, lz4"),
                }
            }
        }
        let mut aliases = HashSet::new();
        let mut addresses = HashSet::new();
        for contract in &self.contracts {
            validate_alias(&contract.alias)?;
            validate_address(&contract.address)?;
            if !aliases.insert(contract.alias.clone()) {
                bail!("duplicate contract alias '{}'", contract.alias);
            }
            if !addresses.insert(contract.address.to_ascii_lowercase()) {
                bail!("duplicate contract address '{}'", contract.address);
            }
        }
        for rule in &self.discovery_rules {
            if !self
                .contracts
                .iter()
                .any(|contract| contract.alias == rule.parent_contract)
            {
                bail!(
                    "parent contract alias '{}' is not configured",
                    rule.parent_contract
                );
            }
            validate_alias(&rule.child_contract)?;
        }
        Ok(())
    }

    pub fn absolute_database(&self, root: &Path) -> PathBuf {
        if self.database.is_absolute() {
            self.database.clone()
        } else {
            root.join(&self.database)
        }
    }
}

pub fn validate_alias(alias: &str) -> Result<()> {
    if alias.is_empty() || !alias.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        bail!("alias must contain only ASCII letters, digits, and underscores")
    }
    Ok(())
}

pub fn validate_address(address: &str) -> Result<()> {
    let raw = address.strip_prefix("0x").unwrap_or(address);
    if raw.len() != 40 || !raw.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("invalid EVM address: {address}")
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_aliases_and_addresses() {
        assert!(validate_alias("usdc_v2").is_ok());
        assert!(validate_alias("usdc-v2").is_err());
        assert!(validate_address("0x0000000000000000000000000000000000000001").is_ok());
        assert!(validate_address("0x1234").is_err());
    }
}
