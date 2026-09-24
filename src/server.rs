use std::collections::HashMap;
use std::io::BufReader;
use std::io::Read;
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::Mutex;

use bincode::config;
use bincode::config::Configuration;
use tracing::instrument;

use crate::worker::WorkerInfo;

struct MasterServer {
    map: HashMap<String, WorkerInfo>,
    config: Configuration,
}
impl MasterServer {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            config: config::standard(),
        }
    }
}

/* covers partral read for us :) */
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
    let master_server = Arc::new(Mutex::new(MasterServer::new()));
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
fn handle_worker_connection(
    master_server: Arc<Mutex<MasterServer>>,
    addr: SocketAddr,
    stream: TcpStream,
) {
    tracing::info!("message received");
    let mut buf_reader = BufReader::new(stream);
    let received = read_stream(&mut buf_reader);
    match received {
        Ok(bytes) => {
            let mut lock = master_server.lock().unwrap(); // TODO handle gracefully
            let (decoded, _): (WorkerInfo, usize) =
                bincode::decode_from_slice(&bytes[..], lock.config).unwrap();
            lock.map.insert(addr.to_string(), decoded);
            /* we dont know if the connection is closed or not? so to remove it from the map, we
             * could use timestamp to remove after threshold!   */
        }
        Err(e) => {
            tracing::info!("{}", e);
        }
    }
}
