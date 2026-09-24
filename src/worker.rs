use bincode::{Decode, Encode};

use crate::Node;

#[derive(Encode, Decode, PartialEq, Debug)]
pub enum WorkerStatus {
    Idle,
    InProgress,
}

#[derive(Encode, Decode, PartialEq, Debug)]
pub struct WorkerInfo {
    pub worker_type: Node,
    pub status: WorkerStatus,
}
