mod sink;
mod source;

use sink::SqliteSink;
use source::EvmEventSource;
use streamling_plugin::{
    init_plugin_with_async_runtime, register_plugin_sink, register_plugin_source,
};

register_plugin_source!("streamling_blockchain", "evm_events", EvmEventSource);
register_plugin_sink!("streamling_blockchain", "sqlite_sink", SqliteSink);
init_plugin_with_async_runtime!();
