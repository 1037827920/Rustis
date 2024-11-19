mod client;
mod cmd;
mod persistence;
mod server;
mod networking;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

pub const DEFAULT_PORT: u16 = 6379;