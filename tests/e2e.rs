use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn spawn(args: &[&str]) -> Child {
    Command::new(env!("CARGO_BIN_EXE_mapreduce"))
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn binary")
}

fn wait_for_line<R: std::io::Read>(
    reader: &mut BufReader<R>,
    needle: &str,
    timeout: Duration,
) -> bool {
    let start = std::time::Instant::now();
    let mut line = String::new();
    while start.elapsed() < timeout {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => std::thread::sleep(Duration::from_millis(50)),
            Ok(_) => {
                if line.contains(needle) {
                    return true;
                }
            }
            Err(_) => std::thread::sleep(Duration::from_millis(50)),
        }
    }
    false
}

#[test]
fn e2e_worker_gets_chunks_and_emits() {
    // use uncommon ports to avoid clashing with a dev master
    let master_port = "1961";
    let worker_port = "1962";

    let mut master = spawn(&["master", "-p", master_port]);
    std::thread::sleep(Duration::from_secs(1));

    let mut worker = spawn(&["worker", "-p", worker_port, "-c", master_port]);
    std::thread::sleep(Duration::from_secs(2));

    let mut client = spawn(&["client", "-p", master_port]);
    let status = client.wait().expect("client wait");
    assert!(status.success(), "client exited fine");

    // worker should log at least one emit pair
    let worker_stdout = worker.stdout.take().expect("worker stdout");
    let mut reader = BufReader::new(worker_stdout);
    assert!(
        wait_for_line(&mut reader, "emit", Duration::from_secs(15)),
        "worker never emitted map output"
    );

    worker.kill().ok();
    master.kill().ok();
}
