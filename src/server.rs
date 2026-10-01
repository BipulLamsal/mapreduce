use std::collections::{HashMap, HashSet, VecDeque};
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

#[derive(Debug, Encode, Decode, Clone)]
pub struct ReduceInfo {
    pub job_id: JobId,
    pub partition: usize,
    pub reduce_fn: u8,
    pub files: Vec<PathBuf>,
}

#[derive(Debug, Encode, Decode, Clone)]
pub enum WorkerTask {
    Map(ChunkInfo),
    Reduce(ReduceInfo),
}

#[derive(Debug, Encode, Decode, Clone)]
pub struct ChunkInfo {
    pub job_id: JobId,
    pub id: ChunkId,
    pub map_fn: u8,
    pub num_partitions: usize,
    pub path: PathBuf,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Encode, Decode)]
pub enum ChunkResult {
    Ok { intermediates: HashSet<PathBuf> },
    Err { reason: String },
}

pub struct ChunkState {
    pub chunk: Arc<ChunkInfo>,
}

enum DispatchEvent {
    ClientConnection(Arc<ChunkInfo>, Sender<(Arc<ChunkInfo>, u16)>),
    ReduceWork(Vec<ReduceInfo>),
    WorkerAvailable(u16),
}

fn send_task(port: u16, task: WorkerTask) {
    let encoded = bincode::encode_to_vec(&task, config::standard()).unwrap();
    tcp_connect(port, |mut s| {
        use std::io::Write as _;
        let _ = s.write_all(&encoded);
    });
    tracing::info!("Sent {:?} to worker : 127.0.0.1:{}", task, port);
}

#[instrument]
fn shedule_dispatcher(dispatcher_rx: Receiver<DispatchEvent>) {
    tracing::debug!("Scheduler running");
    let mut worker_queue = VecDeque::new();
    let mut reducer_queue = VecDeque::new();
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
                if let Some(chunk) = chunk_queue.pop_front() {
                    tracing::debug!("worker availble pulling some chunk");
                    chunk.1.send((chunk.0, avl)).unwrap();
                } else if let Some(task) = reducer_queue.pop_front() {
                    worker_queue.push_back(avl);
                    let port: u16 = worker_queue.pop_front().unwrap();
                    send_task(port, WorkerTask::Reduce(task));
                } else {
                    worker_queue.push_back(avl);
                }
            }
            DispatchEvent::ReduceWork(tasks) => {
                for task in tasks {
                    if let Some(port) = worker_queue.pop_front() {
                        send_task(port, WorkerTask::Reduce(task));
                    } else {
                        reducer_queue.push_back(task);
                    }
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
    ReduceDone {
        job_id: JobId,
        partition: usize,
        worker_port: u16,
        output: String,
    },
}

#[derive(Default)]
struct JobState {
    total: usize,
    reduce_fn: u8,
    num_partitions: usize,
    pending: HashMap<ChunkId, ChunkState>,
    intermediates: HashMap<ChunkId, HashSet<PathBuf>>,
    reduce_pending: HashSet<usize>,
    outputs: HashMap<usize, String>,
}

impl JobState {
    fn add_chunk(&mut self, chunk_id: ChunkId, chunk: &Arc<ChunkInfo>) {
        self.pending.insert(
            chunk_id,
            ChunkState {
                chunk: chunk.clone(),
            },
        );
    }

    fn add_intermediate(&mut self, chunk_id: ChunkId, result: ChunkResult) -> bool {
        match result {
            ChunkResult::Ok { intermediates } => {
                self.intermediates.insert(chunk_id, intermediates);
                self.pending.remove(&chunk_id);
            }
            ChunkResult::Err { .. } => {}
        }
        self.is_map_done()
    }

    fn is_map_done(&self) -> bool {
        self.total > 0 && self.pending.is_empty() && self.intermediates.len() == self.total
    }

    fn take_reduce_tasks(&mut self, job_id: JobId) -> Option<Vec<ReduceInfo>> {
        if !self.is_map_done() {
            return None;
        }

        // groups them based on partion number
        // for simplication we are only accpeting numbers
        // from 0..n
        let n = self.num_partitions.max(1);
        let mut buckets: Vec<Vec<PathBuf>> = vec![Vec::new(); n];
        for path in std::mem::take(&mut self.intermediates)
            .into_values()
            .flatten()
        {
            buckets[partition_of(&path, self.num_partitions) % n].push(path);
        }

        let mut tasks = Vec::new();
        for (partition, files) in buckets.into_iter().enumerate() {
            if files.is_empty() {
                continue;
            }

            self.reduce_pending.insert(partition);
            tasks.push(ReduceInfo {
                job_id,
                partition,
                reduce_fn: self.reduce_fn,
                files,
            });
        }
        Some(tasks)
    }

    fn ack_reduce(&mut self, partition: usize, output: String) -> Option<HashMap<usize, String>> {
        /* remove from pending andd then append on the ouput layer and reuturns after
         * no pending is left*/
        self.reduce_pending.remove(&partition);
        self.outputs.insert(partition, output);

        if self.reduce_pending.is_empty() {
            Some(std::mem::take(&mut self.outputs))
        } else {
            None
        }
    }
}

fn partition_of(path: &PathBuf, _num_partitions: usize) -> usize {
    let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
    name.rsplit('_')
        .next()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0)
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

    fn init_job(&self, job_id: JobId, reduce_fn: u8, num_partitions: usize) {
        let mut jobs = self.jobs.lock().unwrap();
        let entry = jobs.entry(job_id).or_default();
        entry.reduce_fn = reduce_fn;
        entry.num_partitions = num_partitions;
    }

    fn ack_reduce(
        &self,
        job_id: JobId,
        partition: usize,
        output: String,
    ) -> Option<HashMap<usize, String>> {
        let outputs = {
            let mut jobs = self.jobs.lock().unwrap();
            jobs.get_mut(&job_id)?.ack_reduce(partition, output)
        };

        // pending is done
        if outputs.is_some() {
            self.jobs.lock().unwrap().remove(&job_id);
        }
        outputs
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
                        tracing::error!(
                            "chunk {} of job {} failed on worker {}: {}",
                            chunk_id,
                            job_id,
                            worker_port,
                            reason
                        );
                    } else {
                        let tasks = {
                            let mut jobs = master_server.jobs.lock().unwrap();
                            match jobs.get_mut(&job_id) {
                                Some(j) => {
                                    if j.add_intermediate(chunk_id, result) {
                                        j.take_reduce_tasks(job_id)
                                    } else {
                                        None
                                    }
                                }
                                None => None,
                            }
                        };
                        if let Some(tasks) = tasks {
                            tracing::info!(
                                "Ready for reduce for job: {} ({} partitions)",
                                job_id,
                                tasks.len()
                            );
                            handle_reduce_send(master_server.clone(), tasks);
                        }
                    }

                    // worker freed up, tell the dispatcher
                    master_server
                        .dispatcher_tx
                        .send(DispatchEvent::WorkerAvailable(worker_port))
                        .unwrap();
                }
                MasterRecv::ReduceDone {
                    job_id,
                    partition,
                    worker_port,
                    output,
                } => {
                    tracing::info!(
                        "reduce partition {} of job {} acked ({} bytes)",
                        partition,
                        job_id,
                        output.len()
                    );
                    if let Some(outputs) = master_server.ack_reduce(job_id, partition, output) {
                        let mut parts: Vec<_> = outputs.into_iter().collect();
                        parts.sort_by_key(|(p, _)| *p);

                        let merged: String = parts.into_iter().map(|(_, s)| s).collect();
                        let out_path = format!("output_{}.txt", job_id);

                        match std::fs::write(&out_path, &merged) {
                            Ok(_) => {
                                tracing::info!("job {} done, closed. result: {}", job_id, out_path)
                            }
                            Err(e) => tracing::error!(
                                "job {} done but cannot write {}: {}",
                                job_id,
                                out_path,
                                e
                            ),
                        }
                    }
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
                    master_server.init_job(job_id, job.reduce_fn(), job.num_partitions());

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

                    let mut chunks = Vec::new();
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
                            num_partitions: job.num_partitions(),
                            offset,
                            length: read_bytes as u64,
                        });

                        offset += read_bytes as u64;
                        chunks.push((chunk_id, chunk));
                        chunk_id += 1;
                    }

                    tracing::info!("job {} split into {} chunks", job_id, chunk_id);

                    master_server.set_job_chunk_count(job_id, chunk_id as usize);

                    for (cid, chunk) in chunks {
                        master_server.insert_chunk(job_id, cid, &chunk);
                        master_server
                            .dispatcher_tx
                            .send(DispatchEvent::ClientConnection(chunk, notify_tx.clone()))
                            .unwrap();
                    }

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
        send_task(port, WorkerTask::Map((*chunk).clone()));
    }
}

fn handle_reduce_send(server: Arc<MasterServer>, tasks: Vec<ReduceInfo>) {
    server
        .dispatcher_tx
        .send(DispatchEvent::ReduceWork(tasks))
        .unwrap();
}
