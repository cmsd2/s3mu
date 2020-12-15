use serde::{Serialize, Deserialize};
use crate::state::Part;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Action {
    LoadParts,
    StartUpload,
    Terminate,
    Abort {
        upload_id: String,
        attempt: u32,
        msg: String,
    },
    Complete {
        upload_id: String,
        attempt: u32,
    },
    UploadPart {
        upload_id: String,
        index: usize,
        attempt: u32,
        part: Part,
    },
}