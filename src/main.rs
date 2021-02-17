#![type_length_limit = "2000000"]

extern crate configure_me;
#[macro_use]
extern crate serde;

use anyhow::Error;
use btc_rpc_proxy;
// use block_filters;

mod create_state;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let state = create_state::create_state()?.arc();
    btc_rpc_proxy::main(state).await
    // block_filters::main();
}
