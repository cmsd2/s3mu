use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone)]
pub enum Error {
    InvalidState(String),
    Other(String),
    IndexOutOfBounds,
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Part {
    pub number: i64,
    pub path: String,
    pub etag: String,
}

impl Part {
    pub fn new(number: i64, path: String) -> Self {
        Part {
            number,
            path,
            etag: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Operation {
    ConfiguredParts(Vec<Part>),
    Started {
        upload_id: String,
    },
    FailedStart {
        attempt: u32,
        msg: String,
    },
    UploadedPart {
        index: usize,
        etag: String,
    },
    FailedPart {
        index: usize,
        attempt: u32,
        msg: String,
    },
    FailedComplete {
        attempt: u32,
        msg: String,
    },
    Completed,
    FailedAbort {
        attempt: u32,
        msg: String,
    },
    Aborted,
}

#[derive(Debug, Clone)]
pub enum State {
    Init,
    Starting {
        parts: Vec<Part>,
        attempt: u32,
    },
    Uploading {
        parts: Vec<Part>,
        upload_id: String,
        index: usize,
        attempt: u32,
    },
    Completing {
        upload_id: String,
        attempt: u32,
        parts: Vec<Part>,
    },
    Completed,
    Aborting {
        upload_id: String,
        attempt: u32,
    },
    Aborted,
}

impl State {
    pub fn new() -> Self {
        State::Init
    }

    pub fn apply(self, op: Operation) -> Result<State> {
        log::info!("state: {:?}", self);
        log::info!("op: {:?}", op);

        match self {
            State::Init => match op {
                Operation::ConfiguredParts(parts) => {
                    if parts.is_empty() {
                        Err(Error::InvalidState(format!("no parts configured")))
                    } else {
                        Ok(State::Starting { parts, attempt: 0 })
                    }
                },
                op => Err(Error::InvalidState(format!(
                    "invalid operation {:?} in init state",
                    op
                ))),
            },
            State::Starting { parts, attempt } => match op {
                Operation::Started { upload_id } => Ok(State::Uploading {
                    upload_id,
                    parts,
                    index: 0,
                    attempt: 0,
                }),
                Operation::FailedStart { attempt, msg } => Ok(State::Starting {
                    parts,
                    attempt,
                }),
                op => Err(Error::InvalidState(format!(
                    "invalid operation {:?} in ready state",
                    op
                ))),
            },
            State::Uploading {
                mut parts,
                upload_id,
                index,
                attempt,
            } => match op {
                Operation::UploadedPart { mut index, etag } => {
                    parts
                        .get_mut(index)
                        .ok_or_else(|| Error::IndexOutOfBounds)?
                        .etag = etag;
                    
                    index += 1;

                    if index == parts.len() {
                        Ok(State::Completing {
                            upload_id,
                            attempt: 0,
                            parts,
                        })
                    } else {
                        Ok(State::Uploading {
                            upload_id,
                            index,
                            parts,
                            attempt: 0,
                        })
                    }
                }
                Operation::FailedPart {
                    index,
                    attempt,
                    msg,
                } => Ok(State::Uploading {
                    upload_id,
                    index,
                    parts,
                    attempt: attempt + 1,
                }),
                op => Err(Error::InvalidState(format!(
                    "invalid operation {:?} in uploading state",
                    op
                ))),
            },
            State::Completing {
                upload_id,
                attempt,
                parts,
            } => match op {
                Operation::Completed => Ok(State::Completed),
                Operation::FailedComplete { attempt, msg } => Ok(State::Completing {
                    upload_id: upload_id.to_owned(),
                    attempt: attempt + 1,
                    parts,
                }),
                Operation::Aborted => Ok(State::Aborted),
                op => Err(Error::InvalidState(format!(
                    "invalid operation {:?} in completing state",
                    op
                ))),
            },
            State::Aborting {
                ref upload_id,
                attempt,
            } => match op {
                Operation::Aborted => Ok(State::Aborted),
                Operation::FailedAbort { msg, attempt } => Ok(State::Aborting {
                    attempt: attempt + 1,
                    upload_id: upload_id.to_owned(),
                }),
                op => Err(Error::InvalidState(format!(
                    "invalid operation {:?} in aborting state",
                    op
                ))),
            },
            State::Completed => match op {
                op => Err(Error::InvalidState(format!(
                    "invalid operation {:?} in completed state",
                    op
                ))),
            },
            State::Aborted => match op {
                op => Err(Error::InvalidState(format!(
                    "invalid operation {:?} in aborted state",
                    op
                ))),
            }
        }
    }
}
