use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn spawn(args: &[&str]) -> Child {
    Command::new(env!("CARGO_BIN_EXE_mapreduce"))
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn")
}

#[test]
fn output_file_exists() {
    let _ = std::fs::remove_file("output_0.txt");
    let mut master = spawn(&["master", "-p", "1987"]);
    std::thread::sleep(Duration::from_secs(1));
    let mut worker = spawn(&["worker", "-p", "1988", "-c", "1987"]);
    std::thread::sleep(Duration::from_secs(2));

    let mut client = spawn(&["client", "-p", "1987"]);
    assert!(client.wait().unwrap().success());

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if std::fs::metadata("output_0.txt").is_ok() {
            break;
        }
        assert!(Instant::now() < deadline, "output_0.txt never appeared");
        std::thread::sleep(Duration::from_millis(500));
    }

    worker.kill().ok();
    master.kill().ok();
}
