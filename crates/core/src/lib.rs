//! Storage engine and wire protocol shared by the kvforge server and CLI.

mod command;
mod exec;
mod protocol;
mod store;

pub use command::{Command, CommandError};
pub use exec::{execute, is_write};
pub use protocol::{decode, ProtocolError, Value};
pub use store::Store;
