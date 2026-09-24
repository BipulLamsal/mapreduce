use mapreduce::Node;
use mapreduce::State;
use tracing::Level;

fn main() {
    let raw_args = std::env::args();
    let mut args = raw_args.skip(1);
    let mut app_state = State::new(Node::Master, 1900);

    tracing_subscriber::fmt()
        // all spans/events with a level higher than TRACE (e.g, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(Level::TRACE)
        // sets this to be the default, global subscriber for this application.
        .init();

    /*
     * --<options> (port-no)
     * Master(default) MapWorker or ReduceWorker
     * */

    /*
     * This will iterate over the args and picks the last option/value
     * Treat it as a bug or a feature
     * */

    let mut item = args.next();
    while item != None {
        let value = item.as_ref().unwrap();
        if value.starts_with("-") {
            match value.to_lowercase().as_str() {
                "--port" | "-p" => {
                    let number: u16 = args
                        .next()
                        .expect("--port <u16 value>")
                        .parse()
                        .expect("--port <u16 value>");
                    app_state.set_port(number);
                    item = args.next();
                    continue;
                }
                _ => unimplemented!(),
            }
        }

        let node_type: Node = value
            .parse()
            .expect("possible values are : master,map or reduce!");

        app_state.set_type(node_type);
        item = args.next();
    }

    app_state.serve();
}
