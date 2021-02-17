use std::io::{Read, Write};
use std::iter::FromIterator;
use std::net::TcpStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Error;
use async_channel as mpmc;
use bitcoin::{Block, consensus::{Decodable, Encodable}, hash_types::BlockHash, hashes::hex::FromHex, network::{address::Address, constants::{Network::Bitcoin, ServiceFlags}, message::{NetworkMessage, RawNetworkMessage}, message_blockdata::Inventory, message_filter::GetCFilters, message_network::VersionMessage}, util::bip158::{BlockFilter, BlockFilterReader}};
use futures::FutureExt;
use socks::Socks5Stream;
use tokio::time;

use crate::{client::{ClientError, MISC_ERROR_CODE, PRUNE_ERROR_MESSAGE, RpcClient, RpcError, RpcRequest, RpcResponse}, create_state::create_state};
use crate::rpc_methods::{GetBlock, GetBlockFilter, GetBlockParams, GetPeerInfo, PeerAddressError};
use crate::state::{State, TorState};

type VersionMessageProducer = Box<dyn Fn(Address) -> RawNetworkMessage + Send + Sync>;

lazy_static::lazy_static! {
    static ref VER_ACK: RawNetworkMessage = RawNetworkMessage {
        magic: Bitcoin.magic(),
        payload: NetworkMessage::Verack,
    };
    static ref VERSION_MESSAGE: VersionMessageProducer = Box::new(|addr| {
        use std::time::SystemTime;
        RawNetworkMessage {
            magic: Bitcoin.magic(),
            payload: NetworkMessage::Version(VersionMessage::new(
                ServiceFlags::NONE,
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64,
                addr,
                Address::new(&([127, 0, 0, 1], 8332).into(), ServiceFlags::NONE),
                rand::random(),
                format!("BTC RPC Proxy v{}", env!("CARGO_PKG_VERSION")),
                0,
            )),
        }
    });
}

#[derive(Debug)]
pub struct Peers {
    fetched: Option<Instant>,
    pub peers: Vec<Peer>,
}
impl Peers {
    pub fn new() -> Self {
        Peers {
            fetched: None,
            peers: Vec::new(),
        }
    }
    pub fn stale(&self, max_peer_age: Duration) -> bool {
        self.fetched
            .map(|f| f.elapsed() > max_peer_age)
            .unwrap_or(true)
    }
    pub async fn updated(client: &RpcClient) -> Result<Self, PeerUpdateError> {
        println!("updated called!");
        dbg!(
            Ok(Self {
            peers: client
                .call(&RpcRequest {
                    id: None,
                    method: GetPeerInfo,
                    params: [],
                })
                .await?
                .into_result()?
                .into_iter()
                .filter(|p| !p.inbound)
                .filter(|p| p.servicesnames.contains("NETWORK"))
                .filter(|p| p.servicesnames.contains("COMPACT_FILTERS"))
                .map(|p| p.into_address().map(Peer::new))
                .collect::<Result<_, _>>()?,
            fetched: Some(Instant::now()),
            })
        )
    }
    pub fn handles<C: FromIterator<PeerHandle>>(&self) -> C {
        self.peers.iter().map(|p| p.handle()).collect()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PeerUpdateError {
    #[error("Bitcoin RPC failed")]
    Rpc(#[from] RpcError),
    #[error("failed to call Bitcoin RPC")]
    Client(#[from] ClientError),
    #[error("invalid peer address")]
    InvalidPeerAddress(#[from] PeerAddressError),
}

pub enum BitcoinPeerConnection {
    ClearNet(TcpStream),
    Tor(Socks5Stream),
}
impl Read for BitcoinPeerConnection {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            BitcoinPeerConnection::ClearNet(a) => a.read(buf),
            BitcoinPeerConnection::Tor(a) => a.read(buf),
        }
    }
}
impl Write for BitcoinPeerConnection {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            BitcoinPeerConnection::ClearNet(a) => a.write(buf),
            BitcoinPeerConnection::Tor(a) => a.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            BitcoinPeerConnection::ClearNet(a) => a.flush(),
            BitcoinPeerConnection::Tor(a) => a.flush(),
        }
    }
    fn write_vectored(&mut self, bufs: &[std::io::IoSlice<'_>]) -> std::io::Result<usize> {
        match self {
            BitcoinPeerConnection::ClearNet(a) => a.write_vectored(bufs),
            BitcoinPeerConnection::Tor(a) => a.write_vectored(bufs),
        }
    }
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            BitcoinPeerConnection::ClearNet(a) => a.write_all(buf),
            BitcoinPeerConnection::Tor(a) => a.write_all(buf),
        }
    }
    fn write_fmt(&mut self, fmt: std::fmt::Arguments<'_>) -> std::io::Result<()> {
        match self {
            BitcoinPeerConnection::ClearNet(a) => a.write_fmt(fmt),
            BitcoinPeerConnection::Tor(a) => a.write_fmt(fmt),
        }
    }
}
impl BitcoinPeerConnection {
    pub async fn connect(state: Arc<State>, addr: Address) -> Result<Self, Error> {
        tokio::time::timeout(
            state.peer_timeout,
            tokio::task::spawn_blocking(move || {
                let mut stream = match (addr.socket_addr(), &state.tor) {
                    (Ok(addr), Some(TorState { only: false, .. })) | (Ok(addr), None) => {
                        BitcoinPeerConnection::ClearNet(TcpStream::connect(addr)?)
                    }
                    (Ok(addr), Some(tor)) => {
                        BitcoinPeerConnection::Tor(Socks5Stream::connect(tor.proxy, addr)?)
                    }
                    (Err(_), Some(tor)) => BitcoinPeerConnection::Tor(Socks5Stream::connect(
                        tor.proxy,
                        (
                            format!(
                                "{}.onion",
                                base32::encode(
                                    base32::Alphabet::RFC4648 { padding: false },
                                    &addr
                                        .address
                                        .iter()
                                        .map(|n| *n)
                                        .flat_map(|n| u16::to_be_bytes(n).to_vec())
                                        .collect::<Vec<_>>()
                                )
                                .to_lowercase()
                            )
                            .as_str(),
                            addr.port,
                        ),
                    )?),
                    (Err(e), None) => return Err(e.into()),
                };
                VERSION_MESSAGE(addr).consensus_encode(&mut stream)?;
                stream.flush()?;
                let _ =
                    bitcoin::network::message::RawNetworkMessage::consensus_decode(&mut stream)?; // version
                let _ =
                    bitcoin::network::message::RawNetworkMessage::consensus_decode(&mut stream)?; // verack
                VER_ACK.consensus_encode(&mut stream)?;
                stream.flush()?;

                Ok(stream)
            }),
        )
        .await??
    }
}

pub struct Peer {
    addr: Address,
    send: mpmc::Sender<BitcoinPeerConnection>,
    recv: mpmc::Receiver<BitcoinPeerConnection>,
}
impl Peer {
    pub fn new(addr: Address) -> Self {
        let (send, recv) = mpmc::bounded(1);
        Peer { addr, send, recv }
    }
    pub fn handle(&self) -> PeerHandle {
        PeerHandle {
            addr: self.addr.clone(),
            conn: self.recv.try_recv().ok(),
            send: self.send.clone(),
        }
    }
}
impl std::fmt::Debug for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Peer").field("addr", &self.addr).finish()
    }
}

pub struct PeerHandle {
    addr: Address,
    conn: Option<BitcoinPeerConnection>,
    send: mpmc::Sender<BitcoinPeerConnection>,
}
impl PeerHandle {
    pub async fn connect(&mut self, state: Arc<State>) -> Result<RecyclableConnection, Error> {
        if let Some(conn) = self.conn.take() {
            Ok(RecyclableConnection {
                conn,
                send: self.send.clone(),
            })
        } else {
            Ok(RecyclableConnection {
                conn: BitcoinPeerConnection::connect(state, (&self.addr).clone()).await?,
                send: self.send.clone(),
            })
        }
    }
}

pub struct RecyclableConnection {
    conn: BitcoinPeerConnection,
    send: mpmc::Sender<BitcoinPeerConnection>,
}
impl RecyclableConnection {
    fn recycle(self) {
        self.send.try_send(self.conn).unwrap_or_default()
    }
}
impl std::ops::Deref for RecyclableConnection {
    type Target = BitcoinPeerConnection;
    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}
impl std::ops::DerefMut for RecyclableConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

async fn fetch_block_from_self(state: &State, hash: BlockHash) -> Result<Option<Block>, RpcError> {
    match state
        .rpc_client
        .call(&RpcRequest {
            id: None,
            method: GetBlock,
            params: GetBlockParams(hash, Some(0)),
        })
        .await?
        .into_result()
    {
        Ok(b) => Ok(Some(
            Block::consensus_decode(&mut std::io::Cursor::new(
                b.as_left()
                    .ok_or_else(|| anyhow::anyhow!("unexpected response for getblock"))?
                    .as_ref(),
            ))
            .map_err(Error::from)?,
        )),
        Err(e) if e.code == MISC_ERROR_CODE && e.message == PRUNE_ERROR_MESSAGE => Ok(None),
        Err(e) => Err(e),
    }
}

async fn fetch_block_from_peer<'a>(
    state: Arc<State>,
    hash: BlockHash,
    mut conn: RecyclableConnection,
) -> Result<(Block, RecyclableConnection), Error> {
    tokio::time::timeout(state.peer_timeout, async move {
        conn = tokio::task::spawn_blocking(move || {
            RawNetworkMessage {
                magic: Bitcoin.magic(),
                payload: NetworkMessage::GetData(vec![Inventory::Block(hash)]),
            }
            .consensus_encode(&mut *conn)
            .map_err(Error::from)
            .map(|_| conn)
        })
        .await??;

        loop {
            let (msg, conn_) = tokio::task::spawn_blocking(move || {
                RawNetworkMessage::consensus_decode(&mut *conn)
                    .map_err(Error::from)
                    .map(|msg| (msg, conn))
            })
            .await??;
            conn = conn_;
            match msg.payload {
                NetworkMessage::Block(b) => {
                    let returned_hash = b.block_hash();
                    let merkle_check = b.check_merkle_root();
                    let witness_check = b.check_witness_commitment();
                    return match (returned_hash == hash, merkle_check, witness_check) {
                        (true, true, true) => Ok((b, conn)),
                        (true, true, false) => {
                            Err(anyhow::anyhow!("Witness check failed for {:?}", hash))
                        }
                        (true, false, _) => {
                            Err(anyhow::anyhow!("Merkle check failed for {:?}", hash))
                        }
                        (false, _, _) => Err(anyhow::anyhow!(
                            "Expected block hash {:?}, got {:?}",
                            hash,
                            returned_hash
                        )),
                    };
                }
                NetworkMessage::Ping(p) => {
                    conn = tokio::task::spawn_blocking(move || {
                        RawNetworkMessage {
                            magic: Bitcoin.magic(),
                            payload: NetworkMessage::Pong(p),
                        }
                        .consensus_encode(&mut *conn)
                        .map_err(Error::from)
                        .map(|_| conn)
                    })
                    .await??;
                }
                m => warn!(state.logger, "Invalid Message Received: {:?}", m),
            }
        }
    })
    .await?
}

async fn fetch_block_from_peers(
    state: Arc<State>,
    peers: Vec<PeerHandle>,
    hash: BlockHash,
) -> Option<Block> {
    use futures::stream::StreamExt;

    let (send, mut recv) = futures::channel::mpsc::channel(1);
    let fut_unordered: futures::stream::FuturesUnordered<_> =
        peers.into_iter().map(futures::future::ready).collect();
    let state_local = state.clone();
    let runner = fut_unordered
        .then(move |mut peer| {
            let state_local = state_local.clone();
            async move {
                fetch_block_from_peer(
                    state_local.clone(),
                    hash.clone(),
                    peer.connect(state_local).await?,
                )
                .await
            }
        })
        .for_each_concurrent(state.max_peer_concurrency, |block_res| {
            match block_res {
                Ok((block, conn)) => {
                    conn.recycle();
                    send.clone().try_send(block).unwrap_or_default();
                }
                Err(e) => warn!(state.logger, "Error fetching block from peer: {}", e),
            }
            futures::future::ready(())
        });
    let b = futures::select! {
        b = recv.next().fuse() => b,
        _ = runner.boxed().fuse() => None,
    };
    b
}

pub async fn fetch_block(
    state: Arc<State>,
    peers: Vec<PeerHandle>,
    hash: BlockHash,
) -> Result<Option<Block>, RpcError> {
    Ok(match fetch_block_from_self(&*state, hash).await? {
        Some(block) => Some(block),
        None => {
            debug!(
                state.logger,
                "Block is pruned from Core, attempting fetch from peers.";
                "block_hash" => %hash
            );
            if let Some(block) = fetch_block_from_peers(state.clone(), peers, hash).await {
                Some(block)
            } else {
                error!(state.logger, "Could not fetch block from peers."; "block_hash" => %hash);
                None
            }
        }
    })
}


async fn fetch_filter_from_self(state: &State, hash: BlockHash) -> Result<Option<BlockFilter>, RpcError> {
    let result: Result<RpcResponse<GetBlockFilter>, ClientError> = match state
        .rpc_client
        .call(&RpcRequest {
            id: None,
            method: GetBlockFilter,
            params: (hash,),
        })
        .await {
            Ok(res) => {
                // println!("success!");
                Ok(res)
            }
            Err(e) => {
                // println!("error thingy!");
                return Err(RpcError::from(e))
            }
        };
    match result.unwrap()
        .into_result()
    {
        Ok(f) => 
        // Ok(Some(f.filter))
        {
            // println!("Got Some filter in fetch_filter_from_self: {:?}", f);
            // Ok(Some(BlockFilter::new(&f.filter.into_bytes())))
            Ok(Some(BlockFilter::new(&f.filter.into_bytes())))
        }
        // .map_err(Error::from)?
        ,
        Err(e) if e.code == MISC_ERROR_CODE && e.message == PRUNE_ERROR_MESSAGE => Ok(None),
        Err(e) => 
        // Err(e)
        {
            // println!("Got Error fetch_filter_from_self!");
            Err(e)
        }
        ,
    }
}

async fn fetch_filter_from_peer<'a>(
    state: Arc<State>,
    start_height: u32,
    hash: BlockHash,
    mut conn: RecyclableConnection,
) -> Result<(BlockFilter, RecyclableConnection), Error> {
    println!("fetching filter from a peer!");
    tokio::time::timeout(state.peer_timeout, async move {
        conn = tokio::task::spawn_blocking(move || {
            RawNetworkMessage {
                magic: Bitcoin.magic(),
                payload: NetworkMessage::GetCFilters(GetCFilters{filter_type: 0, start_height: start_height, stop_hash: hash}),
            }
            .consensus_encode(&mut *conn)
            .map_err(Error::from)
            .map(|_| conn)
        })
        .await??;

        loop {
            let (msg, conn_) = tokio::task::spawn_blocking(move || {
                RawNetworkMessage::consensus_decode(&mut *conn)
                    .map_err(Error::from)
                    .map(|msg| (msg, conn))
            })
            .await??;
            conn = conn_;
            println!("message payload = {:?}", msg.payload);
            match msg.payload {
                NetworkMessage::CFilter(f) => {
                    let filter = BlockFilter::new(&f.filter);
                    let returned_hash = f.block_hash;
                    return Ok((filter, conn))
                    // let merkle_check = f.check_merkle_root();
                    // let witness_check = f.check_witness_commitment();
                    // return match (returned_hash == hash, merkle_check, witness_check) {
                    //     (true, true, true) => Ok((f, conn)),
                    //     (true, true, false) => {
                    //         Err(anyhow::anyhow!("Witness check failed for {:?}", hash))
                    //     }
                    //     (true, false, _) => {
                    //         Err(anyhow::anyhow!("Merkle check failed for {:?}", hash))
                    //     }
                    //     (false, _, _) => Err(anyhow::anyhow!(
                    //         "Expected block hash {:?}, got {:?}",
                    //         hash,
                    //         returned_hash
                    //     )),
                    // };
                }
                NetworkMessage::Ping(p) => {
                    conn = tokio::task::spawn_blocking(move || {
                        RawNetworkMessage {
                            magic: Bitcoin.magic(),
                            payload: NetworkMessage::Pong(p),
                        }
                        .consensus_encode(&mut *conn)
                        .map_err(Error::from)
                        .map(|_| conn)
                    })
                    .await??;
                }
                m => warn!(state.logger, "Invalid Message Received: {:?}", m),
            }
        }
    })
    .await?
}

pub async fn fetch_filter_from_peers(
    state: Arc<State>,
    peers: Vec<PeerHandle>,
    start_height: u32,
    hash: BlockHash,
) -> Option<BlockFilter> {
    use futures::stream::StreamExt;
    println!("fetching filter from peers!");
    let (send, mut recv) = futures::channel::mpsc::channel(1);
    let fut_unordered: futures::stream::FuturesUnordered<_> =
        peers.into_iter().map(futures::future::ready).collect();
    let state_local = state.clone();
    println!("cloned state_local!");
    let runner = fut_unordered
        .then(move |mut peer| {
            let state_local = state_local.clone();
            println!("...and then #1....");
            async move {
                println!("...and then....");
                fetch_filter_from_peer(
                    state_local.clone(),
                    start_height,
                    hash.clone(),
                    peer.connect(state_local).await?,
                )
                .await
            }
        })
        .for_each_concurrent(state.max_peer_concurrency, |filter_res| {
            match filter_res {
                Ok((filter, conn)) => {
                    conn.recycle();
                    send.clone().try_send(filter).unwrap_or_default();
                }
                Err(e) => warn!(state.logger, "Error fetching filter from peer: {}", e),
            }
            futures::future::ready(())
        });
    let b = futures::select! {
        b = recv.next().fuse() => b,
        _ = runner.boxed().fuse() => None,
    };
    b
}

pub async fn fetch_filter(
    state: Arc<State>,
    peers: Vec<PeerHandle>,
    start_height: u32,
    hash: BlockHash,
) -> Result<Option<BlockFilter>, RpcError> {
    // println!("Fetching filter from self!");
    Ok(match fetch_filter_from_self(&*state, hash).await? {
        Some(filter) => 
        // Some(filter)
        {
            // println!("got Some(filter!) {:?}", filter);
            Some(filter)
        }
        ,
        None => {
            println!("Block filter is pruned from Core, attempting fetch from peers.");
            info!(
                state.logger,
                "Block filter is pruned from Core, attempting fetch from peers.";
                "block_hash" => %hash
            );
            if let Some(block) = fetch_filter_from_peers(state.clone(), peers, start_height, hash).await {
                Some(block)
            } else {
                error!(state.logger, "Could not fetch block from peers."; "block_hash" => %hash);
                None
            }
        },
    })
}


#[tokio::test]
async fn test_peers() {
    let state = create_state().unwrap().arc();
    let mut peers= state.clone().get_peers().await.unwrap();
    // if peers.len() == 0 {
    //     panic!();
    // }
    time::delay_for(Duration::from_secs(10)).await;
    let mut first_peer = &mut peers[0];
    let (filter, conn) = fetch_filter_from_peer(
        state.clone(),
        0,
        BlockHash::from_hex("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f").unwrap(),
        first_peer.connect(state.clone()).await.unwrap()
    ).await.unwrap();
    println!("blockfilter = {:?}", filter);
    // let db: sled::Db = sled::open("bf").unwrap();
    // let utxo_tree = db.open_tree("utxo").unwrap();
    // let bf_tree = db.open_tree("bf").unwrap();
}
