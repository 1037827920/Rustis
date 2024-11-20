pub mod client;
mod cmd;
mod networking;
mod persistence;
pub mod server;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

pub const DEFAULT_PORT: u16 = 6379;
