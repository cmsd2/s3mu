use crate::state::Part;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Action {
    LoadParts,
    StartUpload {
        attempt: u32,
    },
    Terminate,
    Abort {
        upload_id: String,
        attempt: u32,
        msg: String,
    },
    Complete {
        upload_id: String,
        attempt: u32,
        parts: Vec<Part>,
    },
    UploadPart {
        upload_id: String,
        index: usize,
        attempt: u32,
        part: Part,
    },
}
