use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Error;
use slog::Logger;
use tokio::sync::RwLock;

use crate::client::RpcClient;
use crate::fetch_blocks::{PeerHandle, Peers};
use crate::users::Users;

#[derive(Debug)]
pub struct TorState {
    pub proxy: SocketAddr,
    pub only: bool,
}

#[derive(Debug)]
pub struct State {
    pub bind: SocketAddr,
    pub rpc_client: RpcClient,
    pub tor: Option<TorState>,
    pub users: Users,
    pub logger: Logger,
    pub peer_timeout: Duration,
    pub peers: RwLock<Arc<Peers>>,
    pub max_peer_age: Duration,
    pub max_peer_concurrency: Option<usize>,
}
impl State {
    pub fn leak(self) -> &'static Self {
        Box::leak(Box::new(self))
    }
    pub fn arc(self) -> Arc<Self> {
        Arc::new(self)
    }
    pub async fn get_peers(self: Arc<Self>) -> Result<Vec<PeerHandle>, Error> {
        let peers = self.peers.read().await.clone();
        if peers.stale(self.max_peer_age) {
            println!("found stale peers!");
            let join_handle = tokio::task::spawn(async move {
                match Peers::updated(&self.rpc_client).await {
                    Ok(peers) => {
                        println!("got ok peers!");
                        *self.peers.write().await = Arc::new(peers)
                    },
                    Err(error) => {
                        println!("got err peers! {:?}", error);
                        error!(self.logger, "failed to update peers"; "error" => #error)
                    },
                }
            });
            // join_handle.await?;
        }
        // if peers.peers.len() == 0 {
        //     panic!("get_peers got no peers");
        // }
        Ok(peers.handles())
    }
}
