/*user framework to wrap the map and reduce*/

/*
 *
 * user_map(key, chunk) {
 * for word in chunk {
 *  emit(word,"1");
 * }
 * }
 *
 *
 * */

use bincode::{Decode, Encode, config};
use std::{net::TcpStream, path::PathBuf};

#[derive(Encode, Decode, PartialEq, Debug)]
pub struct JobInfo {
    file: String,
    map_fn: u8,
}

impl JobInfo {
    pub fn file_path(&self) -> &String {
        return &self.file;
    }
}

use crate::server::{tcp_connect, write_stream};

#[repr(u8)]
#[derive(Clone)]
pub enum UserMapFn {
    CharMap,
}

impl From<&UserMapFn> for MapFn {
    fn from(value: &UserMapFn) -> Self {
        match value {
            UserMapFn::CharMap => char_map_fn,
        }
    }
}

// emit as a closure to run by our worker node
type EmitFn = dyn FnMut(String, String);
type MapFn = fn(&str, &str, &mut EmitFn);

pub struct Framework {
    map: UserMapFn,
    file: PathBuf,
}

impl Framework {
    pub fn new(map: UserMapFn) -> Self {
        Self {
            map,
            file: PathBuf::new(),
        }
    }

    pub fn attach_file(mut self, path: PathBuf) -> Self {
        self.file = path;
        self
    }

    pub fn send(&self, port: u16) {
        let data = JobInfo {
            file: String::from(self.file.to_str().unwrap()),
            map_fn: self.map.clone() as u8,
        };

        let encoded = bincode::encode_to_vec(&data, config::standard());
        match encoded {
            Ok(data) => {
                let write_fn = |mut tcp_stream: TcpStream| {
                    if let Err(err) = write_stream(&mut tcp_stream, &data) {
                        tracing::error!("{}", err);
                    }
                };

                // needs master port
                tcp_connect(port, write_fn);
            }
            Err(err) => {
                tracing::error!("{}", err);
            }
        }
    }
}

pub fn char_map_fn(key: &str, value: &str, emit: &mut EmitFn) {
    for i in value.chars() {
        emit(i.to_string(), "1".to_string());
    }
}
