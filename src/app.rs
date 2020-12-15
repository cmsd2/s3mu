
use std::path::Path;
use std::mem;
use rusoto_s3::S3Client;
use crate::actions::*;
use crate::wal::*;
use crate::state::*;
use crate::result::Result;
use crate::upload;

pub struct App {
    pub s3client: S3Client,
    pub bucket: String,
    pub key: String,
    pub max_attempts: u32,
    pub log: Wal<Operation>,
    pub state: State,
}

impl App {
    pub async fn new(s3client: S3Client, bucket: &str, key: &str, max_attempts: u32, log_file: &Path) -> Result<Self> {
        let log: Wal<Operation> = Wal::open(log_file).await?;
        let mut state = State::new();

        for entry in log.entries.iter() {
            state = state.apply(entry.action.to_owned())?;
        }

        Ok(App {
            s3client,
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            max_attempts,
            log,
            state,
        })
    }

    pub async fn apply(&mut self, op: Operation) -> Result<()> {
        self.log.append(WalEntry::new(op.clone())).await?;

        let mut temp = State::Aborted;
        mem::swap(&mut temp, &mut self.state);

        temp = temp.apply(op)?;
        mem::swap(&mut temp, &mut self.state);

        Ok(())
    }

    pub fn next_action(&self) -> Action {
        match self.state {
            State::Init => {
                Action::LoadParts
            },
            State::Ready { ref parts} => {
                Action::StartUpload
            },
            State::Started { ref parts, ref upload_id, uploaded} => {
                if count == parts.len() {
                    Action::Complete {
                        upload_id: upload_id.to_owned(),
                        attempt: 1,
                    }
                } else if count > parts.len() {
                    Action::Abort {
                        msg: format!("more parts uploaded than configured"),
                        upload_id: upload_id.to_owned(),
                        attempt: 1,
                    }
                } else {
                    Action::UploadPart {
                        upload_id: upload_id.to_owned(),
                        index: count,
                        attempt: 1,
                        part: parts.get(uploaded).unwrap().to_owned(),
                    }
                }
            },
            State::FailedPart { ref parts, ref upload_id, index, attempt, ref msg } => {
                println!("error uploading part {} on attempt {} of {}: {}", index + 1, attempt, self.max_attempts, msg);
                if attempt == self.max_attempts {
                    Action::Abort {
                        upload_id: upload_id.to_owned(),
                        msg: format!("{} out of {} failures uploading part {}: {}", attempt, self.max_attempts, index + 1, msg),
                        attempt: 1,
                    }
                } else if index >= parts.len() {
                    Action::Abort {
                        upload_id: upload_id.to_owned(),
                        msg: format!("invalid part index {}", index),
                        attempt: 1,
                    }
                } else {
                    Action::UploadPart {
                        upload_id: upload_id.to_owned(),
                        index,
                        attempt: attempt + 1,
                        part: parts.get(index).unwrap().to_owned(),
                    }
                }
            },
            State::FailedComplete {
                attempt,
                ref upload_id,
                ref msg
            } => {
                println!("error completing upload on attempt {} of {}: {}", attempt, self.max_attempts, msg);
                if attempt == self.max_attempts {
                    Action::Abort {
                        upload_id: upload_id.to_owned(),
                        msg: format!("{} out of {} failures completing upload: {}", attempt, self.max_attempts, msg),
                        attempt: 1,
                    }
                } else {
                    Action::Complete {
                        upload_id: upload_id.to_owned(),
                        attempt: attempt + 1,
                    }
                }
            },
            State::Completed => {
                Action::Terminate
            },
            State::FailedAbort {
                ref upload_id,
                attempt,
                ref msg
            } => {
                println!("error aborting upload on attempt {} of {}: {}", attempt, self.max_attempts, msg);
                if attempt == self.max_attempts {
                    Action::Terminate
                } else {
                    Action::Abort {
                        upload_id: upload_id.to_owned(),
                        msg: format!("{} out of {} failures completing upload: {}", attempt, self.max_attempts, msg),
                        attempt: attempt + 1,
                    }
                }
            },
            State::Aborted => {
                Action::Terminate
            },
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            let next_action = self.next_action();

            let op = match next_action {
                Action::Terminate => {
                    break;
                },
                Action::UploadPart {
                    ref upload_id,
                    attempt,
                    index,
                    ref part,
                } => {
                    let part_number = (index + 1) as i64;

                    match upload::upload_part(&self.s3client, &part.path, &self.bucket, &self.key, upload_id, part_number).await {
                        Ok(completed_part) => {
                            Operation::UploadedPart {
                                index,
                                etag: completed_part.e_tag,
                            }
                        },
                        Err(err) => {
                            Operation::FailedPart {
                                index,
                                attempt,
                                msg: format!("{:?}", err),
                            }
                        }
                    }
                },
                Action::Abort {
                    ref upload_id,
                    attempt,
                    ref msg,
                } => {
                    match upload::abort_upload(&self.s3client, &self.bucket, &self.key, upload_id).await {
                        Ok(()) => {
                            Operation::Aborted
                        },
                        Err(err) => {
                            Operation::FailedAbort {
                                msg: format!("error aborting upload: {}", err),
                                attempt,
                            }
                        }
                    }
                },
                Action::StartUpload => {
                    match upload::start_upload(&self.s3client, &self.bucket, &self.key).await {

                    }
                }
            };

            self.apply(op).await?;
        }

        Ok(())
    }
}
