//! Execution contracts outside the lifecycle domain. Vendor output never grants authority.
mod contract;
mod privacy;
pub use privacy::redact_credentials;

pub use contract::*;

#[cfg(unix)]
mod process;
#[cfg(unix)]
pub use process::*;

#[cfg(unix)]
mod runner;
#[cfg(unix)]
pub use runner::*;

#[cfg(unix)]
mod client;
#[cfg(unix)]
pub use client::*;

#[cfg(unix)]
mod snapshot;
#[cfg(unix)]
pub use snapshot::*;

#[cfg(unix)]
mod git_snapshot;
#[cfg(unix)]
pub use git_snapshot::*;

#[cfg(unix)]
mod sandbox;
#[cfg(unix)]
pub use sandbox::*;

#[cfg(unix)]
mod vendor;
#[cfg(unix)]
pub use vendor::*;

#[cfg(unix)]
mod usage;
#[cfg(unix)]
mod vendor_output;
#[cfg(unix)]
pub use usage::{ReportedUsage, reported_usage};
#[cfg(unix)]
pub use vendor_output::*;

#[cfg(unix)]
mod checks;
#[cfg(unix)]
pub use checks::*;

#[cfg(unix)]
mod terminal_receipt;
#[cfg(unix)]
pub use terminal_receipt::*;

#[cfg(unix)]
mod instructions;
#[cfg(unix)]
pub use instructions::*;

#[cfg(unix)]
mod advisory;
#[cfg(unix)]
pub use advisory::*;

#[cfg(unix)]
mod checkpoint;
#[cfg(unix)]
pub use checkpoint::*;

#[cfg(unix)]
mod scoped_advisory;
#[cfg(unix)]
pub use scoped_advisory::*;

#[cfg(unix)]
mod process_identity;
#[cfg(unix)]
pub use process_identity::RunnerProcessIdentity;

mod host_epoch;
pub use host_epoch::HostEpoch;
mod child_identity;
pub use child_identity::ChildIdentity;

mod unstarted_receipt;
pub use unstarted_receipt::*;

#[cfg(unix)]
mod auxiliary_runner;
#[cfg(unix)]
pub use auxiliary_runner::*;

#[cfg(unix)]
mod auxiliary_client;
#[cfg(unix)]
pub use auxiliary_client::*;

mod file_broker;
pub use file_broker::*;

#[cfg(unix)]
mod broker_filesystem;
#[cfg(unix)]
pub use broker_filesystem::*;

#[cfg(unix)]
mod broker_transport;
#[cfg(unix)]
pub use broker_transport::*;

#[cfg(unix)]
mod broker_mcp;
#[cfg(unix)]
pub use broker_mcp::*;

#[cfg(unix)]
mod broker_endpoint;
#[cfg(unix)]
pub use broker_endpoint::*;

#[cfg(unix)]
mod broker_journal_wire;
#[cfg(unix)]
pub use broker_journal_wire::*;

#[cfg(unix)]
mod broker_runner;
#[cfg(unix)]
pub use broker_runner::*;

#[cfg(unix)]
mod broker_receipt;
#[cfg(unix)]
pub use broker_receipt::*;

#[cfg(unix)]
mod broker_client;
#[cfg(unix)]
pub use broker_client::*;

#[cfg(unix)]
mod broker_vendor_policy;
#[cfg(unix)]
pub use broker_vendor_policy::{
    PreparedBrokerVendor, broker_codex_command, codex_broker_tool_options,
};
