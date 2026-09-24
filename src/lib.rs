use crate::server::run_master_coordinator;
use bincode::{Decode, Encode};
use std::str::FromStr;

mod client;
mod framework;
mod server;
mod worker;

#[repr(u8)]
#[derive(Encode, Decode, Debug, PartialEq)]
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
            _ => Err(()),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct State {
    state_type: Node,
    state_port: u16,
}

impl State {
    pub fn new(state_type: Node, port: u16) -> Self {
        State {
            state_type,
            state_port: port,
        }
    }

    pub fn set_port(&mut self, port: u16) {
        self.state_port = port;
    }

    pub fn set_type(&mut self, state_type: Node) {
        self.state_type = state_type;
    }

    pub fn serve(&self) {
        // check if its master or worker
        match self.state_type {
            Node::Master => {
                run_master_coordinator(self.state_port);
            }

            _ => unimplemented!(),
        }
    }
}
