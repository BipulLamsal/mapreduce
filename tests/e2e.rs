use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn spawn(args: &[&str], piped: bool) -> Child {
    Command::new(env!("CARGO_BIN_EXE_mapreduce"))
        .args(args)
        .stdout(if piped { Stdio::piped() } else { Stdio::null() })
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn")
}

#[test]
fn map_phase_reports_ready_for_reduce() {
    let mut master = spawn(&["master", "-p", "1981"], true);
    let mut out = BufReader::new(master.stdout.take().unwrap());
    std::thread::sleep(Duration::from_secs(1));

    let mut worker = spawn(&["worker", "-p", "1982", "-c", "1981"], false);
    std::thread::sleep(Duration::from_secs(2));

    let mut client = spawn(&["client", "-p", "1981"], false);
    assert!(client.wait().unwrap().success());

    let mut line = String::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        line.clear();
        match out.read_line(&mut line) {
            Ok(0) | Err(_) => panic!("master never reported ready for reduce"),
            Ok(_) => {
                if line.contains("Ready for reduce") {
                    break;
                }
            }
        }
        assert!(Instant::now() < deadline, "master never reported ready for reduce");
    }

    worker.kill().ok();
    master.kill().ok();
}
