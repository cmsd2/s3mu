use std::fmt;
use serde::{Serialize, Deserialize};

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
    pub path: String,
    pub etag: String,
}

impl Part {
    pub fn new(path: String) -> Self {
        Part {
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

pub enum State {
    Init,
    Ready {
        parts: Vec<Part>,
    },
    Started {
        parts: Vec<Part>,
        upload_id: String,
        uploaded: usize,
    },
    FailedPart {
        parts: Vec<Part>,
        upload_id: String,
        index: usize,
        attempt: u32,
        msg: String,
    },
    FailedComplete {
        upload_id: String,
        attempt: u32,
        msg: String,
    },
    Completed,
    FailedAbort {
        upload_id: String,
        attempt: u32,
        msg: String,
    },
    Aborted,
}

impl State {
    pub fn new() -> Self {
        State::Init
    }

    pub fn apply(self, op: Operation) -> Result<State> {
        match self {
            State::Init => {
                match op {
                    Operation::ConfiguredParts(parts) => {
                        Ok(State::Ready {
                            parts
                        })
                    },
                    op => {
                        Err(Error::InvalidState(format!("invalid operation {:?} in init state", op)))
                    }
                }
            },
            State::Ready { parts } => {
                match op {
                    Operation::Started {
                        upload_id
                    } => {
                        Ok(State::Started {
                            upload_id,
                            parts,
                            uploaded: 0,
                        })
                    },
                    op => {
                        Err(Error::InvalidState(format!("invalid operation {:?} in ready state", op)))
                    }
                }
            },
            State::Started { mut parts, upload_id, uploaded } => {
                match op {
                    Operation::UploadedPart {index, etag} => {
                        parts.get_mut(index).ok_or_else(|| Error::IndexOutOfBounds)?.etag = etag;
                        Ok(State::Started {
                            upload_id,
                            uploaded: index + 1,
                            parts,
                        })
                    },
                    Operation::FailedPart {index,attempt,msg} => {
                        Ok(State::FailedPart {
                            upload_id,
                            index,
                            parts,
                            attempt,
                            msg,
                        })
                    },
                    Operation::Aborted => {
                        Ok(State::Aborted)
                    },
                    Operation::Completed => {
                        Ok(State::Completed)
                    },
                    Operation::FailedAbort {
                        msg,
                        attempt,
                    } => {
                        Ok(State::FailedAbort {
                            attempt,
                            msg,
                            upload_id,
                        })
                    },
                    Operation::FailedComplete {
                        attempt,
                        msg,
                    } => {
                        Ok(State::FailedComplete {
                            upload_id,
                            attempt,
                            msg,
                        })
                    },
                    op => {
                        Err(Error::InvalidState(format!("invalid operation {:?} in started state", op)))
                    }
                }
            }
        }
    }
}