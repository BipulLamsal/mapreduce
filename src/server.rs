use std::collections::{HashMap, VecDeque};
use std::io::BufReader;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc::{Receiver, Sender};
use std::thread;

use bincode::{Decode, Encode, config};
use tracing::instrument;

use crate::framework::JobInfo;
use crate::worker::{WorkerInfo, WorkerStatus};

// metadata of the file call this a chunk
#[derive(Debug, Encode, Decode)]
struct ChunkInfo {
    path: PathBuf,
    offset: u64,
    length: u64,
}

enum DispatchEvent {
    ClientConnection(ChunkInfo, Sender<(ChunkInfo, u16)>),
    WorkerAvailable(u16),
}

#[instrument]
fn shedule_dispatcher(dispatcher_rx: Receiver<DispatchEvent>) {
    let mut worker_queue = VecDeque::new();
    let mut chunk_queue = VecDeque::new();
    while let Ok(event) = dispatcher_rx.recv() {
        match event {
            DispatchEvent::ClientConnection(chunk_info, notify) => {
                if let Some(value) = worker_queue.pop_front() {
                    notify.send((chunk_info, value)).unwrap();
                } else {
                    chunk_queue.push_back((chunk_info, notify));
                }
            }
            DispatchEvent::WorkerAvailable(avl) => {
                worker_queue.push_back(avl);
                if let Some(chunk) = chunk_queue.pop_front() {
                    // we just pushed so there must be something nothing to worry about unwrap here
                    chunk.1.send((chunk.0, avl)).unwrap();
                }
            }
        }
    }
    tracing::error!("Dispatcher error");
}

#[derive(Debug, Encode, Decode)]
enum MasterRecv {
    Worker(WorkerInfo),
    Job(JobInfo),
}

struct MasterServer {
    /*This is mostly not scalable as our hashamap supports 16 (2^16) workes only */
    map: Arc<Mutex<HashMap<u16, WorkerInfo>>>,
    dispatcher_tx: Sender<DispatchEvent>,
}

impl MasterServer {
    fn new(dispatcher_tx: Sender<DispatchEvent>) -> Self {
        Self {
            map: Arc::new(Mutex::new(HashMap::new())),
            dispatcher_tx,
        }
    }
}

/* covers partial read for us :) */
pub fn read_stream(reader: &mut BufReader<TcpStream>) -> std::io::Result<Vec<u8>> {
    let mut buffer = vec![];
    let mut temp_buffer = [0; 512];

    loop {
        let bytes_read = reader.read(&mut temp_buffer)?;
        if bytes_read == 0 {
            break; // Connection closed
        }
        buffer.extend_from_slice(&temp_buffer[..bytes_read]);
    }

    Ok(buffer)
}

/* covers partial write for us :) */
pub fn write_stream(stream: &mut TcpStream, data: &Vec<u8>) -> std::io::Result<()> {
    let mut total_sent = 0;
    let data_len = data.len();

    while total_sent < data_len {
        let sent = stream.write(&data[total_sent..])?;
        total_sent += sent;
    }

    Ok(())
}

#[instrument]
pub fn run_master_coordinator(port: u16) {
    /*
        tracing::error!("SOMETHING IS SERIOUSLY WRONG!!!");
        tracing::warn!("important informational messages; might indicate an error");
        tracing::info!("general informational messages relevant to users");
        tracing::debug!("diagnostics used for internal debugging of a library or application");
        tracing::trace!("very verbose diagnostic events");
    */

    let socker_addr = SocketAddr::new([127, 0, 0, 1].into(), port as u16);
    let (dispatcher_tx, dispatcher_rx) = std::sync::mpsc::channel();

    // run the scheduler thread
    thread::spawn(move || shedule_dispatcher(dispatcher_rx));

    let master_server = Arc::new(MasterServer::new(dispatcher_tx));
    let listener = std::net::TcpListener::bind(socker_addr)
        .expect("not able to open the master, check the port?");

    /*
     * Our server should be handling
     * Connection Map: []
     *
     * */

    match listener.accept() {
        Ok((tcp_stream, addr)) => {
            // better to use tokio or thread pool here but anyways
            let _ = std::thread::spawn(move || {
                handle_worker_connection(master_server.clone(), addr, tcp_stream);
            });
        }
        Err(v) => {
            tracing::error!("{}", v.to_string());
        }
    }
}

#[instrument(skip(master_server))]
fn handle_worker_connection(master_server: Arc<MasterServer>, addr: SocketAddr, stream: TcpStream) {
    tracing::info!("message received");
    let mut buf_reader = BufReader::new(stream);
    let received = read_stream(&mut buf_reader);
    match received {
        Ok(bytes) => {
            let (decoded, _): (MasterRecv, usize) =
                bincode::decode_from_slice(&bytes[..], config::standard()).unwrap();

            match decoded {
                MasterRecv::Worker(worker) => {
                    let port = addr.port();
                    {
                        let mut map_lock = master_server.map.lock().unwrap(); // TODO handle gracefully
                        let worker_status = worker.status;
                        map_lock.insert(port, worker);
                        drop(map_lock);

                        if worker_status == WorkerStatus::Idle {
                            master_server
                                .dispatcher_tx
                                .send(DispatchEvent::WorkerAvailable(port))
                                .unwrap();
                        }
                    }
                }
                MasterRecv::Job(job) => {
                    let (notify_tx, notify_rx) = std::sync::mpsc::channel();

                    let mut buffer = [0; 64]; // 1024/64 = 16 chunks 
                    let path = PathBuf::from(job.file_path());
                    let mut file_open = std::fs::File::open(path).unwrap();
                    let mut offset = 0;
                    loop {
                        let read_bytes = file_open.read(&mut buffer).unwrap();

                        if read_bytes == 0 {
                            break;
                        }

                        let chunk = ChunkInfo {
                            path: PathBuf::from(job.file_path()),
                            offset,
                            length: read_bytes as u64,
                        };

                        offset += read_bytes as u64;

                        master_server
                            .dispatcher_tx
                            .send(DispatchEvent::ClientConnection(chunk, notify_tx.clone()))
                            .unwrap();
                    }
                    handle_chunk_allocation(notify_rx);
                }
            }

            /* we dont know if the connection is closed or not? so to remove it from the map, we
             * could use timestamp to remove after threshold!   */
        }
        Err(e) => {
            tracing::error!("{}", e);
        }
    }
}

pub fn tcp_connect(port: u16, write_fn: impl Fn(TcpStream)) {
    let addr = SocketAddr::new([127, 0, 0, 1].into(), port);
    let status = std::net::TcpStream::connect(addr);
    match status {
        Ok(stream) => {
            write_fn(stream);
        }
        Err(err) => {
            tracing::error!("{}", err);
        }
    }
}

fn handle_chunk_allocation(receiver: Receiver<(ChunkInfo, u16)>) {
    while let Ok((chunk, port)) = receiver.recv() {
        let write_fn = |mut tcp_stream: TcpStream| {
            let encoded = bincode::encode_to_vec(&chunk, config::standard()).unwrap();
            tcp_stream.write(&encoded).unwrap();
        };

        tcp_connect(port, write_fn);
    }
}
