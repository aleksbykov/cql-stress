//! Strongly consistent (Raft-per-tablet) keyspaces: what every cql-stress frontend needs to
//! check before it runs against one.

mod protocol_extensions;

pub use protocol_extensions::fetch_protocol_features;
