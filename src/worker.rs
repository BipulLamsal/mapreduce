use std::{
    io::{BufReader, Read, Seek, SeekFrom, Write},
    net::{SocketAddr, TcpStream},
    sync::{Arc, Mutex},
};

use bincode::{Decode, Encode, config};
use tracing::instrument;

use crate::{
    Node,
    framework::map_fn_from_id,
    server::{ChunkInfo, MasterRecv, read_stream, tcp_connect},
};

#[derive(Encode, Decode, PartialEq, Debug, Clone, Copy)]
pub enum WorkerStatus {
    Idle,
    InProgress,
}

#[derive(Encode, Decode, PartialEq, Debug, Clone, Copy)]
pub struct WorkerInfo {
    pub worker_type: Node,
    pub status: WorkerStatus,
    pub port: u16,
}

#[instrument]
pub fn run_worker(serv_port: u16, connect_port: u16) {
    let serv_addr = SocketAddr::new([127, 0, 0, 1].into(), serv_port as u16);

    let worker_state = Arc::new(Mutex::new(WorkerInfo {
        worker_type: Node::Worker,
        status: WorkerStatus::Idle,
        port: serv_port,
    }));

    let _ = std::thread::spawn(move || send_heart_beat(worker_state.clone(), connect_port));
    let listener = std::net::TcpListener::bind(serv_addr)
        .expect("not able to open the master, check the port?");

    for stream in listener.incoming() {
        match stream {
            Ok(tcp_stream) => {
                // accept the chunk metadata and read and implemnet the map
                let mut reader = BufReader::new(tcp_stream);
                let bytes = match read_stream(&mut reader) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::error!("read chunk failed: {}", e);
                        continue;
                    }
                };
                let (chunk, _): (ChunkInfo, usize) =
                    match bincode::decode_from_slice(&bytes[..], config::standard()) {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::error!("decode chunk failed: {}", e);
                            continue;
                        }
                    };
                tracing::info!("chunk received: {:?}", chunk);
                run_map_chunk(&chunk);
            }
            Err(v) => {
                tracing::error!("{}", v.to_string());
            }
        }
    }
}

fn run_map_chunk(chunk: &ChunkInfo) {
    let mut file = match std::fs::File::open(&chunk.path) {
        Ok(f) => f,
        Err(e) => {
            tracing::error!("cannot open {:?}: {}", chunk.path, e);
            return;
        }
    };

    if let Err(e) = file.seek(SeekFrom::Start(chunk.offset)) {
        tracing::error!("seek failed: {}", e);
        return;
    }

    let mut buf = vec![0u8; chunk.length as usize];
    if let Err(e) = file.read_exact(&mut buf) {
        tracing::error!("read chunk failed: {}", e);
        return;
    }

    let value = String::from_utf8_lossy(&buf).to_string();
    let key = format!("{}:{}", chunk.path.display(), chunk.offset);
    let map_fn = map_fn_from_id(chunk.map_fn);

    let mut emit = |k: String, v: String| {
        // just trace it
        tracing::info!("emit ({:?}, {:?})", k, v);
    };

    map_fn(&key, &value, &mut emit);
}

pub fn send_heart_beat(worker_state: Arc<Mutex<WorkerInfo>>, connect_port: u16) {
    loop {
        let write_fn = |mut tcp_stream: TcpStream| {
            let info = worker_state.lock().unwrap().clone();
            let encoded =
                bincode::encode_to_vec(MasterRecv::Worker(info), config::standard()).unwrap();
            tcp_stream.write(&encoded).unwrap();
        };

        tcp_connect(connect_port, write_fn);
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}
