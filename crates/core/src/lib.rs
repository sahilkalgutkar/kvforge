//! Storage engine and wire protocol shared by the kvforge server and CLI.

mod command;
mod protocol;
mod store;

pub use command::{Command, CommandError};
pub use protocol::{decode, ProtocolError, Value};
pub use store::Store;
