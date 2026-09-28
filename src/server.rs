use std::collections::{HashMap, VecDeque};
use std::io::BufReader;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::thread;

use bincode::{Decode, Encode, config};
use tracing::instrument;

use crate::framework::JobInfo;
use crate::worker::{WorkerInfo, WorkerStatus};

pub const CHUNK_SIZE: u64 = 64;
pub type JobId = u64;
pub type ChunkId = u64;

#[derive(Debug, Encode, Decode)]
pub struct ChunkInfo {
    pub job_id: JobId,
    pub id: ChunkId,
    pub map_fn: u8,
    pub path: PathBuf,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Encode, Decode)]
pub enum ChunkResult {
    Ok { intermediate_path: PathBuf },
    Err { reason: String },
}

pub struct ChunkState {
    pub chunk: Arc<ChunkInfo>,
}

enum DispatchEvent {
    ClientConnection(Arc<ChunkInfo>, Sender<(Arc<ChunkInfo>, u16)>),
    WorkerAvailable(u16),
}

#[instrument]
fn shedule_dispatcher(dispatcher_rx: Receiver<DispatchEvent>) {
    tracing::debug!("Scheduler running");
    let mut worker_queue = VecDeque::new();
    let mut chunk_queue = VecDeque::new();
    while let Ok(event) = dispatcher_rx.recv() {
        match event {
            DispatchEvent::ClientConnection(chunk_info, notify) => {
                tracing::debug!("event received for client chunk");
                if let Some(value) = worker_queue.pop_front() {
                    tracing::debug!("worker availble for chunk");
                    notify.send((chunk_info, value)).unwrap();
                } else {
                    tracing::debug!("worker unavailble for chunk");
                    chunk_queue.push_back((chunk_info, notify));
                }
            }
            DispatchEvent::WorkerAvailable(avl) => {
                tracing::debug!("event received for availble worker");
                worker_queue.push_back(avl);
                if let Some(chunk) = chunk_queue.pop_front() {
                    // we just pushed so there must be something nothing to worry about unwrap here
                    tracing::debug!("worker availble pulling some chunk");
                    chunk.1.send((chunk.0, avl)).unwrap();
                }
            }
        }
    }
    tracing::error!("Dispatcher error");
}

#[derive(Debug, Encode, Decode)]
pub enum MasterRecv {
    Worker(WorkerInfo),
    Job(JobInfo),
    TaskDone {
        job_id: JobId,
        chunk_id: ChunkId,
        worker_port: u16,
        result: ChunkResult,
    },
}

#[derive(Default)]
struct JobState {
    total: usize,
    pending: HashMap<ChunkId, ChunkState>,
    intermediates: HashMap<ChunkId, PathBuf>,
}

impl JobState {
    fn add_chunk(&mut self, chunk_id: ChunkId, chunk: &Arc<ChunkInfo>) {
        self.pending
            .insert(chunk_id, ChunkState { chunk: chunk.clone() });
    }

    fn add_intermediate(&mut self, chunk_id: ChunkId, result: ChunkResult) -> Option<Vec<PathBuf>> {
        match result {
            ChunkResult::Ok { intermediate_path } => {
                self.intermediates.insert(chunk_id, intermediate_path);
                self.pending.remove(&chunk_id);
            }
            ChunkResult::Err { .. } => {}
        }

        if !self.is_map_done() {
            return None;
        }

        // drain rather than clone: reduce must be triggered at most once
        Some(
            std::mem::take(&mut self.intermediates)
                .into_values()
                .collect(),
        )
    }

    fn is_map_done(&self) -> bool {
        self.total > 0 && self.pending.is_empty() && self.intermediates.len() == self.total
    }
}

struct MasterServer {
    /*This is mostly not scalable as our hashamap supports 16 (2^16) workes only */
    map: Arc<Mutex<HashMap<u16, WorkerInfo>>>,
    dispatcher_tx: Sender<DispatchEvent>,
    job_id_counter: AtomicUsize,
    jobs: Arc<Mutex<HashMap<JobId, JobState>>>,
}

impl MasterServer {
    fn new(dispatcher_tx: Sender<DispatchEvent>) -> Self {
        Self {
            map: Arc::new(Mutex::new(HashMap::new())),
            dispatcher_tx,
            job_id_counter: AtomicUsize::new(0),
            jobs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn insert_chunk(&self, job_id: JobId, chunk_id: ChunkId, chunk: &Arc<ChunkInfo>) {
        let mut jobs = self.jobs.lock().unwrap();
        jobs.entry(job_id).or_default().add_chunk(chunk_id, chunk);
    }

    fn get_intermediates(
        &self,
        job_id: JobId,
        chunk_id: ChunkId,
        result: ChunkResult,
    ) -> Option<Vec<PathBuf>> {
        let mut jobs = self.jobs.lock().unwrap();
        jobs.get_mut(&job_id)?.add_intermediate(chunk_id, result)
    }

    fn set_job_chunk_count(&self, job_id: JobId, count: usize) {
        if let Some(job) = self.jobs.lock().unwrap().get_mut(&job_id) {
            job.total = count;
        }
    }

    fn register_worker(&self, worker: WorkerInfo) -> bool {
        let idle = worker.status == WorkerStatus::Idle;
        self.map.lock().unwrap().insert(worker.port, worker);
        idle
    }

    fn notify_worker_available(&self, port: u16) {
        self.dispatcher_tx
            .send(DispatchEvent::WorkerAvailable(port))
            .unwrap();
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

    let listener = match std::net::TcpListener::bind(socker_addr) {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("cannot bind master on {}: {}", socker_addr, e);
            return;
        }
    };

    tracing::info!("master listening on {}", socker_addr);

    /*
     * Our server should be handling
     * Connection Map: []
     *
     * */

    loop {
        match listener.accept() {
            Ok((tcp_stream, addr)) => {
                let master_server = master_server.clone();
                // better to use tokio or thread pool here but anyways
                let _ = std::thread::spawn(move || {
                    handle_worker_connection(master_server.clone(), addr, tcp_stream);
                });
            }
            Err(v) => {
                tracing::error!("{}", v.to_string());
                break;
            }
        }
    }
}

#[instrument(skip(master_server))]
fn handle_worker_connection(master_server: Arc<MasterServer>, addr: SocketAddr, stream: TcpStream) {
    let mut buf_reader = BufReader::new(stream);
    let received = read_stream(&mut buf_reader);
    tracing::debug!("raw bytes received: {:?}", received);

    match received {
        Ok(bytes) => {
            let (decoded, _): (MasterRecv, usize) =
                bincode::decode_from_slice(&bytes[..], config::standard()).unwrap();

            match decoded {
                MasterRecv::Worker(worker) => {
                    tracing::info!("Worker Addr: {}", addr.to_string());
                    let port = worker.port;
                    if master_server.register_worker(worker) {
                        master_server.notify_worker_available(port);
                    }
                }
                MasterRecv::TaskDone {
                    job_id,
                    chunk_id,
                    worker_port,
                    result,
                } => {
                    tracing::info!("chunk {} of job {} acked", chunk_id, job_id);

                    if let ChunkResult::Err { reason } = &result {
                        // the chunk stays outstanding; the retry scan (once it
                        // exists) is what re-dispatches it
                        tracing::error!(
                            "chunk {} of job {} failed on worker {}: {}",
                            chunk_id,
                            job_id,
                            worker_port,
                            reason
                        );
                    }

                    if let Some(intermediates) =
                        master_server.get_intermediates(job_id, chunk_id, result)
                    {
                        tracing::info!(
                            "Ready for reduce for job: {} for {}",
                            job_id,
                            intermediates.len()
                        );
                    }

                    // worker freed up, tell the dispatcher
                    master_server
                        .dispatcher_tx
                        .send(DispatchEvent::WorkerAvailable(worker_port))
                        .unwrap();
                }
                MasterRecv::Job(job) => {
                    tracing::info!("Client Addr: {}", addr.to_string());

                    let (notify_tx, notify_rx) = std::sync::mpsc::channel();
                    let job_id =
                        master_server.job_id_counter.fetch_add(1, Ordering::SeqCst) as JobId;
                    let mut chunk_id: ChunkId = 0;
                    let mut buffer = vec![0u8; CHUNK_SIZE as usize];

                    let path = PathBuf::from(job.file_path());
                    let mut file_open = match std::fs::File::open(&path) {
                        Ok(f) => f,
                        Err(e) => {
                            tracing::error!("cannot open {:?}: {}", path, e);

                            return;
                        }
                    };
                    let mut offset = 0;

                    loop {
                        let read_bytes = match file_open.read(&mut buffer) {
                            Ok(0) => break,
                            Ok(n) => n,
                            Err(e) => {
                                tracing::error!("cannot read {:?}: {}", path, e);
                                break;
                            }
                        };

                        let chunk = Arc::new(ChunkInfo {
                            job_id,
                            id: chunk_id,
                            path: path.clone(),
                            map_fn: job.map_fn(),
                            offset,
                            length: read_bytes as u64,
                        });

                        offset += read_bytes as u64;

                        master_server.insert_chunk(job_id, chunk_id, &chunk);
                        master_server
                            .dispatcher_tx
                            .send(DispatchEvent::ClientConnection(chunk, notify_tx.clone()))
                            .unwrap();

                        chunk_id += 1;
                    }

                    tracing::info!("job {} split into {} chunks", job_id, chunk_id);
                    master_server.set_job_chunk_count(job_id, chunk_id as usize);

                    drop(notify_tx);
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

fn handle_chunk_allocation(receiver: Receiver<(Arc<ChunkInfo>, u16)>) {
    while let Ok((chunk, port)) = receiver.recv() {
        let write_fn = |mut tcp_stream: TcpStream| {
            let encoded = bincode::encode_to_vec(&chunk, config::standard()).unwrap();
            tcp_stream.write(&encoded).unwrap();
        };

        tcp_connect(port, write_fn);
        tracing::info!("Sent chunk to worker : 127.0.0.1:{}", port);
    }
}
