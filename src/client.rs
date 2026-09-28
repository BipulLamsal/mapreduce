use std::path::PathBuf;

use crate::framework::Framework;
use crate::framework::UserMapFn;
use crate::framework::UserReduceFn;

pub fn run_client(port: u16) {
    /* so for this rpc call we need a shared mechism so we can address the same function  */
    let rpc = Framework::new(UserMapFn::CharMap, UserReduceFn::CharMap, 2); // two pations are set
    let path = PathBuf::from("README.md");
    rpc.attach_file(path).send(port);
}
