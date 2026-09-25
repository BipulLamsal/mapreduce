use std::{
    io::Write,
    net::{SocketAddr, TcpStream},
    sync::{Arc, Mutex},
};

use bincode::{Decode, Encode, config};
use tracing::instrument;

use crate::{Node, server::tcp_connect};

#[derive(Encode, Decode, PartialEq, Debug, Clone, Copy)]
pub enum WorkerStatus {
    Idle,
    InProgress,
}

#[derive(Encode, Decode, PartialEq, Debug)]
pub struct WorkerInfo {
    pub worker_type: Node,
    pub status: WorkerStatus,
}

#[instrument]
pub fn run_worker(serv_port: u16, connect_port: u16) {
    let serv_addr = SocketAddr::new([127, 0, 0, 1].into(), serv_port as u16);

    let worker_state = Arc::new(Mutex::new(WorkerInfo {
        worker_type: Node::Worker,
        status: WorkerStatus::Idle,
    }));

    let _ = std::thread::spawn(move || send_heart_beat(worker_state.clone(), connect_port));
    let listener = std::net::TcpListener::bind(serv_addr)
        .expect("not able to open the master, check the port?");

    match listener.accept() {
        Ok((tcp_stream, addr)) => {

            // accept the chunk metadata and read and implemnet the map
        }
        Err(v) => {
            tracing::error!("{}", v.to_string());
        }
    }
}

pub fn send_heart_beat(worker_state: Arc<Mutex<WorkerInfo>>, connect_port: u16) {
    loop {
        let write_fn = |mut tcp_stream: TcpStream| {
            let encoded = bincode::encode_to_vec(&worker_state, config::standard()).unwrap();
            tcp_stream.write(&encoded).unwrap();
        };

        tcp_connect(connect_port, write_fn);
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}
