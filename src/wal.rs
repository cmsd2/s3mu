use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::Path;
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufStream};

pub type Result<T> = std::result::Result<T, WalError>;

#[derive(Debug)]
pub enum WalError {
    LoadError(String),
    AppendError(String),
}

impl std::error::Error for WalError {}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Deserialize, Serialize)]
pub struct WalEntry<Action> {
    pub action: Action,
}

impl <A: Serialize + DeserializeOwned> WalEntry<A> {
    pub fn new(action: A) -> Self {
        WalEntry {
            action,
        }
    }
}

pub struct Wal<Action> {
    pub stream: BufStream<fs::File>,
    pub entries: Vec<WalEntry<Action>>,
}

impl<A: Serialize + DeserializeOwned + 'static> Wal<A> {
    pub async fn open(file_path: &Path) -> Result<Self> {
        let f = fs::File::open(file_path)
            .await
            .map_err(|err| WalError::LoadError(format!("error opening log: {}", err)))?;

        let stream = BufStream::new(f);

        let mut entries = vec![];

        let mut lines = stream.lines();
        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|err| WalError::LoadError(format!("error reading log: {}", err)))?
        {
            let entry = serde_json::from_str(&line)
                .map_err(|err| WalError::LoadError(format!("error deserialising log: {}", err)))?;
            entries.push(entry);
        }

        Ok(Wal {
            stream: lines.into_inner(),
            entries,
        })
    }

    pub async fn append(&mut self, entry: WalEntry<A>) -> Result<()> {
        let line = serde_json::to_string(&entry).map_err(|err| {
            WalError::AppendError(format!("error serialising log entry: {}", err))
        })?;

        self.stream
            .write_all(line.as_bytes())
            .await
            .map_err(|err| WalError::AppendError(format!("error writing log entry: {}", err)))?;

        self.stream
            .flush()
            .await
            .map_err(|err| WalError::AppendError(format!("error flushing log: {}", err)))?;

        Ok(())
    }
}
