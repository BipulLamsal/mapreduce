use crate::{client::run_client, server::run_master_coordinator, worker::run_worker};
use bincode::{Decode, Encode};
use std::str::FromStr;

mod client;
mod framework;
mod server;
mod worker;

pub const DEFAULT_MASTER_PORT: u16 = 1900;

#[repr(u8)]
#[derive(Encode, Decode, Debug, PartialEq, Clone, Copy)]
pub enum Node {
    Master,
    Worker,
    Client,
}

impl FromStr for Node {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "master" => Ok(Self::Master),
            "worker" => Ok(Self::Worker),
            "client" => Ok(Self::Client),
            _ => Err(()),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct State {
    state_type: Node,
    state_port: u16,
    connect_port: u16, // applicable to worker
}

impl State {
    pub fn new(state_type: Node, port: u16) -> Self {
        State {
            state_type,
            state_port: port,
            connect_port: DEFAULT_MASTER_PORT, // default master port
        }
    }

    pub fn set_port(&mut self, port: u16) {
        self.state_port = port;
    }

    pub fn set_type(&mut self, state_type: Node) {
        self.state_type = state_type;
    }

    pub fn set_connect(&mut self, connect_port: u16) {
        self.connect_port = connect_port;
    }

    pub fn serve(&self) {
        // check if its master or worker
        match self.state_type {
            Node::Master => {
                run_master_coordinator(self.state_port);
            }
            Node::Client => run_client(self.state_port),
            Node::Worker => {
                run_worker(self.state_port, self.connect_port);
            }
        }
    }
}
