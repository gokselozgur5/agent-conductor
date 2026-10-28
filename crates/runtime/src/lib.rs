//! Execution contracts outside the lifecycle domain. Vendor output never grants authority.
mod contract;

pub use contract::*;

#[cfg(unix)]
mod process;
#[cfg(unix)]
pub use process::*;
