//! EVM log decoding shared by the event source and `replay`, so the fields one stores are
//! the fields the other checks.

use alloy_dyn_abi::{DynSolEvent, DynSolValue, Specifier};
use alloy_json_abi::JsonAbi;
use serde_json::{Map, Value};
use std::{collections::HashMap, fmt, path::Path, str::FromStr};

pub use alloy_json_abi::Event;
pub use alloy_primitives::B256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// A topic several ABIs declare, from an address that none of them owns.
    Ambiguous(String),
    /// An unreadable ABI or a log that does not match its event.
    Invalid(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ambiguous(message) | Self::Invalid(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {}

fn invalid(error: impl fmt::Display) -> Error {
    Error::Invalid(error.to_string())
}

/// One contract's event, with its types resolved once.
#[derive(Clone, Debug)]
pub struct EventDescriptor {
    pub owner: String,
    pub event: Event,
    resolved: DynSolEvent,
}

impl EventDescriptor {
    pub fn new(owner: impl Into<String>, event: Event) -> Result<Self, Error> {
        let resolved = event
            .resolve()
            .map_err(|error| invalid(format!("event {}: {error}", event.name)))?;
        Ok(Self {
            owner: owner.into(),
            event,
            resolved,
        })
    }
}

/// Events by topic0. One topic can belong to several contracts' ABIs.
pub type EventIndex = HashMap<B256, Vec<EventDescriptor>>;

/// Parses an ABI and checks that every event resolves and has a topic0. Event types are
/// rewritten to canonical names, so `uint` hashes as `uint256` in the topic0.
pub fn parse_abi(json: &[u8], source: &str) -> Result<JsonAbi, Error> {
    let mut abi: JsonAbi = serde_json::from_slice(json)
        .map_err(|error| invalid(format!("parse {source}: {error}")))?;
    for event in abi.events.values_mut().flatten() {
        for input in &mut event.inputs {
            input.ty = canonical_type(&input.ty);
            input.components.iter_mut().for_each(canonical_param);
        }
    }
    for event in abi.events() {
        if event.anonymous {
            return Err(invalid(format!(
                "anonymous event {} in {source} is unsupported; Streamling topic filters require topic0",
                event.name
            )));
        }
        EventDescriptor::new("", event.clone())?;
    }
    Ok(abi)
}

fn canonical_param(param: &mut alloy_json_abi::Param) {
    param.ty = canonical_type(&param.ty);
    param.components.iter_mut().for_each(canonical_param);
}

/// `uint` and `int` with any array suffix, as `uint256` and `int256`.
fn canonical_type(ty: &str) -> String {
    let (base, suffix) = ty.split_at(ty.find('[').unwrap_or(ty.len()));
    match base {
        "uint" => format!("uint256{suffix}"),
        "int" => format!("int256{suffix}"),
        _ => ty.to_owned(),
    }
}

/// Reads the events of the ABI at `path`.
pub fn read_abi_events(path: &Path) -> Result<Vec<Event>, Error> {
    let json = std::fs::read(path)
        .map_err(|error| invalid(format!("read {}: {error}", path.display())))?;
    Ok(parse_abi(&json, &path.display().to_string())?
        .events()
        .cloned()
        .collect())
}

/// Adds `owner`'s events to `index` and returns their topics.
pub fn index_events(
    owner: &str,
    events: Vec<Event>,
    index: &mut EventIndex,
) -> Result<Vec<B256>, Error> {
    let mut topics = Vec::with_capacity(events.len());
    for event in events {
        let topic = event.selector();
        index
            .entry(topic)
            .or_default()
            .push(EventDescriptor::new(owner, event)?);
        topics.push(topic);
    }
    Ok(topics)
}

/// The column name of an event input: its ABI name, or `unnamed_<position>`.
pub fn field_name(index: usize, name: &str) -> String {
    if name.is_empty() {
        format!("unnamed_{}", index + 1)
    } else {
        name.to_owned()
    }
}

/// A log decoded with its owner's ABI.
#[derive(Debug)]
pub struct DecodedLog<'a> {
    pub descriptor: &'a EventDescriptor,
    /// Lowercase emitting address.
    pub address: String,
    pub topic0: String,
    pub block_number: u64,
    pub block_hash: String,
    pub tx_hash: String,
    pub log_index: u64,
    /// Inputs in ABI order, as JSON.
    pub fields: Map<String, Value>,
    values: HashMap<String, DynSolValue>,
}

impl DecodedLog<'_> {
    pub fn event_id(&self, chain_id: u64) -> String {
        format!("{chain_id}:{}:{}", self.block_hash, self.log_index)
    }

    /// The lowercase address in input `name`, if that input is an address.
    pub fn address_input(&self, name: &str) -> Option<String> {
        match self.values.get(name)? {
            DynSolValue::Address(address) => Some(format!("{address:#x}")),
            _ => None,
        }
    }
}

/// Decodes one `eth_getLogs` result. `owners` maps lowercase addresses to contract aliases;
/// the first map that knows the address wins. A known contract's log decodes only with that
/// contract's ABI, so a topic its ABI does not declare (for example an inherited
/// `RoleGranted`) is skipped, not borrowed from another ABI.
pub fn decode_log<'a>(
    log: &Value,
    index: &'a EventIndex,
    owners: &[&HashMap<String, String>],
) -> Result<Option<DecodedLog<'a>>, Error> {
    let topics = log
        .get("topics")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let Some(topic0) = topics.first().and_then(Value::as_str) else {
        return Ok(None);
    };
    let Some(candidates) = index.get(&B256::from_str(topic0).map_err(invalid)?) else {
        return Ok(None);
    };
    let address = text(log, "address").to_ascii_lowercase();
    let owner = owners.iter().find_map(|owners| owners.get(&address));
    let descriptor = match owner {
        Some(owner) => match candidates
            .iter()
            .find(|candidate| &candidate.owner == owner)
        {
            Some(descriptor) => descriptor,
            None => return Ok(None),
        },
        None if candidates.len() == 1 => &candidates[0],
        None => {
            return Err(Error::Ambiguous(format!(
                "ambiguous event topic {topic0} for address {address}"
            )));
        }
    };
    let raw_topics = topics
        .iter()
        .filter_map(Value::as_str)
        .map(B256::from_str)
        .collect::<Result<Vec<_>, _>>()
        .map_err(invalid)?;
    let data = hex::decode(text(log, "data").trim_start_matches("0x")).map_err(invalid)?;
    let decoded = descriptor
        .resolved
        .decode_log_parts(raw_topics, &data)
        .map_err(|error| invalid(format!("decode {}: {error}", descriptor.event.name)))?;
    let mut indexed = decoded.indexed.into_iter();
    let mut body = decoded.body.into_iter();
    let mut fields = Map::new();
    let mut values = HashMap::new();
    for (position, input) in descriptor.event.inputs.iter().enumerate() {
        let value = if input.indexed {
            indexed.next()
        } else {
            body.next()
        }
        .ok_or_else(|| invalid(format!("decode {}: missing input", descriptor.event.name)))?;
        let name = field_name(position, &input.name);
        fields.insert(name.clone(), value_json(&value));
        values.insert(name, value);
    }
    Ok(Some(DecodedLog {
        descriptor,
        address,
        topic0: topic0.to_owned(),
        block_number: parse_hex(text(log, "blockNumber"))?,
        block_hash: text(log, "blockHash").to_owned(),
        tx_hash: text(log, "transactionHash").to_owned(),
        log_index: parse_hex(text(log, "logIndex"))?,
        fields,
        values,
    }))
}

fn text<'a>(log: &'a Value, key: &str) -> &'a str {
    log.get(key).and_then(Value::as_str).unwrap_or_default()
}

/// A JSON-RPC hex quantity; empty means zero.
pub fn parse_hex(text: &str) -> Result<u64, Error> {
    let digits = text.trim_start_matches("0x");
    if digits.is_empty() {
        return Ok(0);
    }
    u64::from_str_radix(digits, 16).map_err(|error| invalid(format!("hex {text:?}: {error}")))
}

/// The stored JSON form of a value: integers as decimal strings (signed for `intN`),
/// bytes and addresses as lowercase `0x` hex, and arrays and tuples as JSON arrays.
pub fn value_json(value: &DynSolValue) -> Value {
    match value {
        DynSolValue::Address(v) => format!("{v:#x}").into(),
        DynSolValue::Function(v) => format!("0x{}", hex::encode(v)).into(),
        DynSolValue::FixedBytes(v, size) => format!("0x{}", hex::encode(&v[..*size])).into(),
        DynSolValue::Bytes(v) => format!("0x{}", hex::encode(v)).into(),
        DynSolValue::Int(v, _) => v.to_string().into(),
        DynSolValue::Uint(v, _) => v.to_string().into(),
        DynSolValue::Bool(v) => (*v).into(),
        DynSolValue::String(v) => v.clone().into(),
        DynSolValue::Array(v) | DynSolValue::FixedArray(v) | DynSolValue::Tuple(v) => {
            Value::Array(v.iter().map(value_json).collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn word(hex: &str) -> String {
        format!("0x{hex:0>64}")
    }

    fn index(owner: &str, abi: Value) -> EventIndex {
        let mut index = EventIndex::new();
        let events = parse_abi(abi.to_string().as_bytes(), "test")
            .unwrap()
            .events()
            .cloned()
            .collect();
        index_events(owner, events, &mut index).unwrap();
        index
    }

    #[test]
    fn decodes_inputs_in_abi_order_with_stored_formats() {
        let index = index(
            "pool",
            json!([{
                "type": "event", "name": "Swap", "anonymous": false,
                "inputs": [
                    {"name": "sender", "type": "address", "indexed": true},
                    {"name": "amount", "type": "int256", "indexed": false},
                    {"name": "", "type": "bytes4", "indexed": false},
                    {"name": "tag", "type": "string", "indexed": true},
                    {"name": "ok", "type": "bool", "indexed": false},
                ]
            }]),
        );
        let topic = *index.keys().next().unwrap();
        assert_eq!(
            topic,
            alloy_primitives::keccak256("Swap(address,int256,bytes4,string,bool)")
        );
        let sender = "00000000000000000000000000000000000000000000000000000000000000ab";
        let tag = "cd".repeat(32);
        let data = format!(
            "0x{}{:0<64}{}",
            "f".repeat(64),
            "deadbeef",
            word("1").trim_start_matches("0x"),
        );
        let log = json!({
            "address": "0x1111111111111111111111111111111111111111",
            "topics": [format!("{topic:#x}"), format!("0x{sender}"), format!("0x{tag}")],
            "data": data,
            "blockNumber": "0x10",
            "blockHash": "0xblock",
            "transactionHash": "0xtx",
            "logIndex": "0x2",
        });
        let owners = HashMap::from([(
            "0x1111111111111111111111111111111111111111".to_owned(),
            "pool".to_owned(),
        )]);
        let decoded = decode_log(&log, &index, &[&owners]).unwrap().unwrap();
        assert_eq!(decoded.descriptor.owner, "pool");
        assert_eq!(decoded.event_id(1), "1:0xblock:2");
        assert_eq!(decoded.block_number, 16);
        assert_eq!(
            Value::Object(decoded.fields.clone()).to_string(),
            json!({
                "sender": "0x00000000000000000000000000000000000000ab",
                "amount": "-1",
                "unnamed_3": "0xdeadbeef",
                "tag": format!("0x{tag}"),
                "ok": true,
            })
            .to_string()
        );
        assert_eq!(
            decoded.address_input("sender").as_deref(),
            Some("0x00000000000000000000000000000000000000ab")
        );
        assert_eq!(decoded.address_input("amount"), None);
    }

    #[test]
    fn owners_choose_the_abi_and_unknown_shared_topics_are_ambiguous() {
        let abi = json!([{"type": "event", "name": "Ping", "anonymous": false, "inputs": []}]);
        let mut both = index("a", abi.clone());
        let b = index("b", abi);
        for (topic, descriptors) in b {
            both.entry(topic).or_default().extend(descriptors);
        }
        let topic = *both.keys().next().unwrap();
        let log = |address: &str| {
            json!({"address": address, "topics": [format!("{topic:#x}")], "data": "0x",
                   "blockNumber": "0x1", "blockHash": "0xh", "logIndex": "0x0"})
        };
        let direct = HashMap::from([("0xaa".to_owned(), "x".to_owned())]);
        let children = HashMap::from([("0xbb".to_owned(), "b".to_owned())]);
        let owners = [&direct, &children];
        let chosen = decode_log(&log("0xBB"), &both, &owners).unwrap().unwrap();
        assert_eq!(chosen.descriptor.owner, "b");
        // A known contract whose ABI lacks the topic: skipped.
        assert!(decode_log(&log("0xaa"), &both, &owners).unwrap().is_none());
        assert!(matches!(
            decode_log(&log("0xcc"), &both, &owners),
            Err(Error::Ambiguous(_))
        ));
    }

    #[test]
    fn type_aliases_hash_as_canonical_types() {
        let index = index(
            "t",
            json!([{"type": "event", "name": "Test", "anonymous": false, "inputs": [
                {"name": "a", "type": "uint", "indexed": false},
                {"name": "b", "type": "int[2]", "indexed": false},
                {"name": "c", "type": "tuple[]", "indexed": false,
                 "components": [{"name": "d", "type": "uint", "components": []}]},
            ]}]),
        );
        assert_eq!(
            *index.keys().next().unwrap(),
            alloy_primitives::keccak256("Test(uint256,int256[2],(uint256)[])")
        );
    }

    #[test]
    fn rejects_anonymous_events_and_unknown_types() {
        let anonymous = json!([{"type": "event", "name": "A", "anonymous": true, "inputs": []}]);
        assert!(
            parse_abi(anonymous.to_string().as_bytes(), "x.json")
                .unwrap_err()
                .to_string()
                .contains("anonymous event A in x.json")
        );
        let bad = json!([{"type": "event", "name": "B", "anonymous": false,
                          "inputs": [{"name": "v", "type": "uint7", "indexed": false}]}]);
        assert!(parse_abi(bad.to_string().as_bytes(), "x.json").is_err());
    }
}
