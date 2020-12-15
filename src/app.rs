use crate::actions::*;
use crate::result::Result;
use crate::state::*;
use crate::upload;
use crate::wal::*;
use rusoto_s3::S3Client;
use std::mem;
use std::path::{Path, PathBuf};

pub struct App {
    pub s3client: S3Client,
    pub bucket: String,
    pub key: String,
    pub max_attempts: u32,
    pub log: Wal<Operation>,
    pub state: State,
    pub pattern: String,
}

impl App {
    pub async fn new(
        s3client: S3Client,
        bucket: &str,
        key: &str,
        max_attempts: u32,
        log_file: &Path,
        pattern: &str,
    ) -> Result<Self> {
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
            pattern: pattern.to_owned(),
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
            State::Init => Action::LoadParts,
            State::Starting { ref parts, attempt } => {
                if attempt == self.max_attempts {
                    Action::Terminate
                } else {
                    Action::StartUpload {
                        attempt: attempt,
                    }
                }
            },
            State::Uploading {
                ref parts,
                ref upload_id,
                index,
                attempt,
            } => {
                log::info!(
                    "uploading part {} attempt {} of {}",
                    index + 1,
                    attempt,
                    self.max_attempts,
                );
                if attempt == self.max_attempts {
                    Action::Abort {
                        upload_id: upload_id.to_owned(),
                        msg: format!(
                            "{} out of {} failures uploading part {}",
                            attempt,
                            self.max_attempts,
                            index + 1,
                        ),
                        attempt: 1,
                    }
                } else {
                    Action::UploadPart {
                        upload_id: upload_id.to_owned(),
                        index,
                        attempt: attempt,
                        part: parts.get(index).unwrap().to_owned(),
                    }
                }
            }
            State::Completing {
                attempt,
                ref upload_id,
                ref parts,
            } => {
                log::info!(
                    "completing upload attempt {} of {}",
                    attempt, self.max_attempts
                );
                if attempt == self.max_attempts {
                    Action::Abort {
                        upload_id: upload_id.to_owned(),
                        msg: format!(
                            "{} out of {} failures completing upload",
                            attempt, self.max_attempts
                        ),
                        attempt: 1,
                    }
                } else {
                    Action::Complete {
                        upload_id: upload_id.to_owned(),
                        attempt: attempt,
                        parts: parts.to_owned(),
                    }
                }
            }
            State::Completed => Action::Terminate,
            State::Aborting {
                ref upload_id,
                attempt,
            } => {
                log::info!(
                    "aborting upload attempt {} of {}",
                    attempt, self.max_attempts
                );
                if attempt == self.max_attempts {
                    Action::Terminate
                } else {
                    Action::Abort {
                        upload_id: upload_id.to_owned(),
                        msg: format!(
                            "{} out of {} failures completing upload",
                            attempt, self.max_attempts
                        ),
                        attempt: attempt,
                    }
                }
            }
            State::Aborted => Action::Terminate,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            let next_action = self.next_action();

            log::info!("action: {:?}", next_action);

            let op = match next_action {
                Action::Terminate => {
                    break;
                },
                Action::LoadParts => {
                    let mut parts = vec![];
                    let paths = upload::get_parts(&self.pattern).map_err(|err| format!("get part files error: {}", err))?;
                    let mut i = 1;
                    for path in paths {
                        parts.push(Part::new(i, path.to_str().ok_or_else(|| format!("error handling non utf8 path"))?.to_owned()));
                        i += 1;
                    }
                    Operation::ConfiguredParts(parts)
                },
                Action::UploadPart {
                    ref upload_id,
                    attempt,
                    index,
                    ref part,
                } => {
                    let part_number = (index + 1) as i64;

                    match upload::upload_part(
                        &self.s3client,
                        &PathBuf::from(&part.path),
                        &self.bucket,
                        &self.key,
                        upload_id,
                        part_number,
                    )
                    .await
                    .map_err(|err| format!("upload part error: {:?}", err))
                    .and_then(|part| part.e_tag.ok_or_else(|| format!("missing etag in uploaded part")))
                    {
                        Ok(etag) => Operation::UploadedPart {
                            index,
                            etag: etag,
                        },
                        Err(err) => Operation::FailedPart {
                            index,
                            attempt,
                            msg: format!("{:?}", err),
                        },
                    }
                },
                Action::Abort {
                    ref upload_id,
                    attempt,
                    ref msg,
                } => {
                    match upload::abort_upload(&self.s3client, &self.bucket, &self.key, upload_id)
                        .await
                    {
                        Ok(()) => Operation::Aborted,
                        Err(err) => Operation::FailedAbort {
                            msg: format!("error aborting upload: {}", err),
                            attempt,
                        },
                    }
                },
                Action::StartUpload {
                    attempt,
                } => {
                    match upload::start_upload(&self.s3client, &self.bucket, &self.key).await {
                        Ok(upload_id) => {
                            Operation::Started {
                                upload_id,
                            }
                        },
                        Err(err) => {
                            Operation::FailedStart {
                                attempt: attempt,
                                msg: format!("error starting upload: {}", err),
                            }
                        }
                    }
                },
                Action::Complete {
                    ref upload_id,
                    attempt,
                    ref parts,
                } => {
                    let completed_upload = rusoto_s3::CompletedMultipartUpload {
                        parts: Some(parts.iter().map(|part| rusoto_s3::CompletedPart {
                            e_tag: Some(part.etag.to_owned()),
                            part_number: Some(part.number),
                        }).collect()),
                    };
                    match upload::complete_upload(&self.s3client, &self.bucket, &self.key, upload_id, completed_upload)
                        .await
                    {
                        Ok(()) => Operation::Completed,
                        Err(err) => Operation::FailedComplete {
                            msg: format!("error aborting upload: {}", err),
                            attempt,
                        },
                    }
                }
            };

            self.apply(op).await?;
        }

        Ok(())
    }
}
