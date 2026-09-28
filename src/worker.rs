use std::{
    io::{BufReader, Read, Seek, SeekFrom, Write},
    net::{SocketAddr, TcpStream},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use bincode::{Decode, Encode, config};
use tracing::instrument;

use crate::{
    Node,
    framework::map_fn_from_id,
    server::{ChunkInfo, ChunkResult, MasterRecv, read_stream, tcp_connect, write_stream},
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

    let heartbeat_state = Arc::clone(&worker_state);
    let _ = std::thread::spawn(move || send_heart_beat(heartbeat_state, connect_port));
    let listener = match std::net::TcpListener::bind(serv_addr) {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("cannot bind worker on {}: {}", serv_addr, e);
            return;
        }
    };

    tracing::info!("worker listening on {}", serv_addr);

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

                worker_state.lock().unwrap().status = WorkerStatus::InProgress;

                let result = run_map_chunk(&chunk);

                worker_state.lock().unwrap().status = WorkerStatus::Idle;
                reply_ack_to_master(&chunk, serv_port, connect_port, result);
            }
            Err(v) => {
                tracing::error!("{}", v.to_string());
            }
        }
    }
}

fn reply_ack_to_master(
    chunk: &ChunkInfo,
    worker_port: u16,
    connect_port: u16,
    result: ChunkResult,
) {
    let msg = MasterRecv::TaskDone {
        job_id: chunk.job_id,
        chunk_id: chunk.id,
        worker_port,
        result,
    };
    let encoded = bincode::encode_to_vec(&msg, config::standard()).unwrap();
    let write_fn = |mut tcp_stream: TcpStream| {
        if let Err(e) = write_stream(&mut tcp_stream, &encoded) {
            tracing::error!("failed to ack chunk {}: {}", chunk.id, e);
        }
    };
    tcp_connect(connect_port, write_fn);
}

fn run_map_chunk(chunk: &ChunkInfo) -> ChunkResult {
    let mut file = match std::fs::File::open(&chunk.path) {
        Ok(f) => f,
        Err(e) => {
            return ChunkResult::Err {
                reason: format!("cannot open {:?}: {}", chunk.path, e),
            };
        }
    };

    if let Err(e) = file.seek(SeekFrom::Start(chunk.offset)) {
        return ChunkResult::Err {
            reason: format!("seek failed: {}", e),
        };
    }

    let mut buf = vec![0u8; chunk.length as usize];
    if let Err(e) = file.read_exact(&mut buf) {
        return ChunkResult::Err {
            reason: format!("read chunk failed: {}", e),
        };
    }

    let value = String::from_utf8_lossy(&buf).to_string();
    let key = format!("{}:{}", chunk.path.display(), chunk.offset);
    let map_fn = map_fn_from_id(chunk.map_fn);

    let intermediate_path = PathBuf::from(format!("intermediate/{}_{}", chunk.job_id, chunk.id));

    let mut emit = {
        let out_path = intermediate_path.clone();
        move |k: String, v: String| {
            tracing::info!("emit ({:?}, {:?})", k, v);
            if let Some(parent) = out_path.parent()
                && let Err(e) = std::fs::create_dir_all(parent)
            {
                tracing::error!("cannot create intermediate dir: {}", e);
                return;
            }
            if let Err(e) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&out_path)
                .and_then(|mut f| writeln!(f, "{}\t{}", k, v))
            {
                tracing::error!("cannot write intermediate output: {}", e);
            }
        }
    };

    map_fn(&key, &value, &mut emit);

    ChunkResult::Ok { intermediate_path }
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
