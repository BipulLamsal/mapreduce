use std::path::PathBuf;

use crate::framework::Framework;
use crate::framework::UserMapFn;

fn client_main(port: u16) {
    /* so for this rpc call we need a shared mechism so we can address the same function  */
    let rpc = Framework::new(UserMapFn::CharMap);
    let path = PathBuf::from("../README.md");
    rpc.attach_file(path).send(port);
}
