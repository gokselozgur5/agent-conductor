use core::fmt;

use crate::{ActorRole, TaskState};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Rejection {
    VersionConflict {
        expected: u64,
        actual: u64,
    },
    IllegalTransition {
        from: TaskState,
        command: &'static str,
    },
    Unauthorized {
        role: ActorRole,
        command: &'static str,
    },
    InvalidLease(&'static str),
    InvalidCommand(&'static str),
    VerificationIncomplete(&'static str),
}

impl fmt::Display for Rejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for Rejection {}
