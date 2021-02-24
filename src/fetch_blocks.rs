use std::io::{Read, Write};
use std::iter::FromIterator;
use std::net::TcpStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Error;
use async_channel as mpmc;
use bitcoin::{Block, FilterHash, FilterHeader, consensus::{Decodable, Encodable}, hash_types::BlockHash, hashes::hex::{FromHex, ToHex}, network::{address::Address, constants::{Network::Bitcoin, ServiceFlags}, message::{NetworkMessage, RawNetworkMessage}, message_blockdata::Inventory, message_filter::{GetCFCheckpt, GetCFHeaders, GetCFilters}, message_network::VersionMessage}, util::bip158::{BlockFilter, BlockFilterReader}};
use futures::FutureExt;
use socks::Socks5Stream;
use tokio::time;

use crate::{client::{ClientError, MISC_ERROR_CODE, PRUNE_ERROR_MESSAGE, RpcClient, RpcError, RpcRequest, RpcResponse}, create_state::create_state, rpc_methods::GetBlockHash};
use crate::rpc_methods::{GetBlock, GetBlockFilter, GetBlockParams, GetPeerInfo, PeerAddressError};
use crate::state::{State, TorState};

type VersionMessageProducer = Box<dyn Fn() -> RawNetworkMessage + Send + Sync>;

lazy_static::lazy_static! {
    static ref VER_ACK: RawNetworkMessage = RawNetworkMessage {
        magic: Bitcoin.magic(),
        payload: NetworkMessage::Verack,
    };
    static ref VERSION_MESSAGE: VersionMessageProducer = Box::new(|| {
        use std::time::SystemTime;
        RawNetworkMessage {
            magic: Bitcoin.magic(),
            payload: NetworkMessage::Version(VersionMessage::new(
                ServiceFlags::NONE,
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64,
                bitcoin::network::Address::new(&([127, 0, 0, 1], 8332).into(), ServiceFlags::NONE),
                bitcoin::network::Address::new(&([127, 0, 0, 1], 8332).into(), ServiceFlags::NONE),
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
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }
    pub async fn updated(client: &RpcClient) -> Result<Self, PeerUpdateError> {
        // println!("updated called!");
        // dbg!(
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
                .map(|p| Peer::new(Arc::new(p.addr)))
                .collect(),
            fetched: Some(Instant::now()),
            })
        // )
    }
    pub fn handles<C: FromIterator<PeerHandle>>(&self) -> C {
        // println!("peers.handles... len = {}", self.peers.len());
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

#[derive(Debug)]
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
    pub async fn connect(state: Arc<State>, addr: Arc<String>) -> Result<Self, Error> {
        // println!("BitcoinPeerConnection::connect");
        tokio::time::timeout(
            state.peer_timeout,
            tokio::task::spawn_blocking(move || {
                // println!("spawn_blocking task started!");
                // if addr.spilt()
                let mut stream = match &state.tor {
                    Some(TorState { only, proxy })
                        if *only || addr.split(":").next().unwrap().ends_with(".onion") =>
                    {
                        // println!("TOR! {:?}", addr);
                        BitcoinPeerConnection::Tor(Socks5Stream::connect(proxy, &**addr)?)
                    },
                    _ => {
                        // println!("CLEARNET! {:?}", addr);
                        BitcoinPeerConnection::ClearNet(TcpStream::connect(&*addr)?)
                    },
                };
                // println!("spawn_blocking task about to encode version_message!");
                VERSION_MESSAGE().consensus_encode(&mut stream)?;
                stream.flush()?;
                // println!("flushed stream!");
                let _ =
                    bitcoin::network::message::RawNetworkMessage::consensus_decode(&mut stream)?; // version
                let _ =
                    bitcoin::network::message::RawNetworkMessage::consensus_decode(&mut stream)?; // verack
                VER_ACK.consensus_encode(&mut stream)?;
                stream.flush()?;
                // println!("spawn_blocking task finished!");
                Ok(stream)
            }),
        )
        .await??
    }
}

pub struct Peer {
    addr: Arc<String>,
    send: mpmc::Sender<BitcoinPeerConnection>,
    recv: mpmc::Receiver<BitcoinPeerConnection>,
}
impl Peer {
    pub fn new(addr: Arc<String>) -> Self {
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
    addr: Arc<String>,
    conn: Option<BitcoinPeerConnection>,
    send: mpmc::Sender<BitcoinPeerConnection>,
}
impl PeerHandle {
    pub async fn connect(&mut self, state: Arc<State>) -> Result<RecyclableConnection, Error> {
        if let Some(conn) = self.conn.take() {
            // println!("ok!");
            Ok(RecyclableConnection {
                conn,
                send: self.send.clone(),
            })
        } else {
            // println!("not ok! {:?}", (&self.addr).clone());
            Ok(RecyclableConnection {
                conn: BitcoinPeerConnection::connect(state, (&self.addr).clone()).await?,
                send: self.send.clone(),
            })
        }
    }
}

#[derive(Debug)]
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

async fn fetch_filters_from_peer<'a>(
    state: Arc<State>,
    start_height: u32,
    stop_hash: BlockHash,
    mut conn: RecyclableConnection,
) -> Result<(Vec<(BlockHash, BlockFilter)>, RecyclableConnection), Error> {
    // println!("fetching filter from a peer!");
    tokio::time::timeout(state.peer_timeout, async move {
        conn = tokio::task::spawn_blocking(move || {
            RawNetworkMessage {
                magic: Bitcoin.magic(),
                payload: NetworkMessage::GetCFilters(GetCFilters{filter_type: 0, start_height: start_height, stop_hash}),
            }
            .consensus_encode(&mut *conn)
            .map_err(Error::from)
            .map(|_| conn)
        })
        .await??;

        let mut filters = Vec::<(BlockHash, BlockFilter)>::new();

        loop {
            let (msg, conn_) = tokio::task::spawn_blocking(move || {
                RawNetworkMessage::consensus_decode(&mut *conn)
                    .map_err(Error::from)
                    .map(|msg| (msg, conn))
            })
            .await??;
            conn = conn_;
            // println!("message payload = {:?}", msg.payload);
            match msg.payload {
                NetworkMessage::CFilter(f) => {
                    // println!("got CFilter response!");
                    let filter = BlockFilter::new(&f.filter);
                    let hash = f.block_hash;
                    filters.push((hash, filter));
                    if hash == stop_hash {
                        return Ok((filters, conn))
                    }
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

pub async fn fetch_filters_from_peers(
    state: Arc<State>,
    peers: Vec<PeerHandle>,
    start_height: u32,
    stop_hash: BlockHash,
) -> Option<Vec<(BlockHash, BlockFilter)>> {
    use futures::stream::StreamExt;
    // println!("fetching filter from peers!");
    let (send, mut recv) = futures::channel::mpsc::channel(1);
    let fut_unordered: futures::stream::FuturesUnordered<_> =
        peers.into_iter().map(futures::future::ready).collect();
    let state_local = state.clone();
    // println!("cloned state_local!");
    let runner = fut_unordered
        .then(move |mut peer| {
            let state_local = state_local.clone();
            // println!("...and then #1....");
            async move {
                println!("fetching filters from peer: {:?}", peer.addr);
                fetch_filters_from_peer(
                    state_local.clone(),
                    start_height,
                    stop_hash.clone(),
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


async fn fetch_cf_checkpts_from_peer<'a>(
    state: Arc<State>,
    stop_hash: BlockHash,
    mut conn: RecyclableConnection,
) -> Result<(Vec<FilterHeader>, RecyclableConnection), Error> {
    // println!("fetching filter from a peer!");
    tokio::time::timeout(state.peer_timeout, async move {
        conn = tokio::task::spawn_blocking(move || {
            RawNetworkMessage {
                magic: Bitcoin.magic(),
                payload: NetworkMessage::GetCFCheckpt(GetCFCheckpt{filter_type: 0, stop_hash}),
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
            // println!("message payload = {:?}", msg.payload);
            match msg.payload {
                NetworkMessage::CFCheckpt(checkpts) => {
                    return Ok((checkpts.filter_headers, conn))
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


pub async fn fetch_cf_checkpts_from_peers(
    state: Arc<State>,
    peers: Vec<PeerHandle>,
    stop_hash: BlockHash,
) -> Option<Vec<FilterHeader>> {
    use futures::stream::StreamExt;
    // println!("fetching filter from peers!");
    let (send, mut recv) = futures::channel::mpsc::channel(1);
    let fut_unordered: futures::stream::FuturesUnordered<_> =
        peers.into_iter().map(futures::future::ready).collect();
    let state_local = state.clone();
    // println!("cloned state_local!");
    let runner = fut_unordered
        .then(move |mut peer| {
            let state_local = state_local.clone();
            // println!("...and then #1....");
            async move {
                println!("fetching filters from peer: {:?}", peer.addr);
                fetch_cf_checkpts_from_peer(
                    state_local.clone(),
                    stop_hash.clone(),
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

async fn fetch_cf_headers_from_peer<'a>(
    state: Arc<State>,
    start_height: u32,
    stop_hash: BlockHash,
    mut conn: RecyclableConnection,
) -> Result<(Vec<FilterHeader>, RecyclableConnection), Error> {
    // println!("fetching filter from a peer!");
    tokio::time::timeout(state.peer_timeout, async move {
        conn = tokio::task::spawn_blocking(move || {
            RawNetworkMessage {
                magic: Bitcoin.magic(),
                payload: NetworkMessage::GetCFHeaders(GetCFHeaders{filter_type: 0, start_height, stop_hash}),
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
            // println!("message payload = {:?}", msg.payload);
            match msg.payload {
                NetworkMessage::CFHeaders(cfh_response) => {
                    // verify filter hashes
                    let mut headers = Vec::<FilterHeader>::new();
                    println!("previous header = {}", cfh_response.previous_filter_header);
                    let mut current_header = cfh_response.previous_filter_header;
                    // if start_height == 1 {
                    //     headers.push(current_header);
                    // }
                    // headers.pop();
                    for filter_hash in cfh_response.filter_hashes {
                        current_header = filter_hash.filter_header(&current_header);
                        headers.push(current_header);
                    }
                    return Ok((headers, conn))
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


pub async fn fetch_cf_headers_from_peers(
    state: Arc<State>,
    peers: Vec<PeerHandle>,
    start_height: u32,
    stop_hash: BlockHash,
) -> Option<Vec<FilterHeader>> {
    use futures::stream::StreamExt;
    // println!("fetching filter from peers!");
    let (send, mut recv) = futures::channel::mpsc::channel(1);
    let fut_unordered: futures::stream::FuturesUnordered<_> =
        peers.into_iter().map(futures::future::ready).collect();
    let state_local = state.clone();
    // println!("cloned state_local!");
    let runner = fut_unordered
        .then(move |mut peer| {
            let state_local = state_local.clone();
            // println!("...and then #1....");
            async move {
                println!("fetching filters from peer: {:?}", peer.addr);
                fetch_cf_headers_from_peer(
                    state_local.clone(),
                    start_height,
                    stop_hash.clone(),
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

async fn get_block_hash(state: Arc<State>, block_height: usize) -> Result<BlockHash, RpcError> {
    loop {
        let result = match state
        .rpc_client
        .call(&RpcRequest {
            id: None,
            method: GetBlockHash,
            params: (block_height,),
        })
        .await {
            Ok(a) => return a.into_result(),
            Err(e) => {
                eprintln!("error getting block hash, retrying: {}", e);
                std::thread::sleep(std::time::Duration::from_secs(1));
            ()
            },
        };
    }
}

#[tokio::test]
async fn test_fetch_and_verify_filters() {
    let state = create_state().unwrap().arc();
    let mut peers= state.clone().get_peers().await.unwrap();
    let mut first_peer = &mut peers[0];
    let (checkpts, conn) = 
        fetch_cf_checkpts_from_peer(
            state.clone(),
            BlockHash::from_hex("0000000000000000000a94e99abfcac55d72702dbc967414fdb6f067ca76d969").unwrap(), // 671830
            first_peer.connect(state.clone()).await.unwrap()
        ).await.unwrap();
    let mut conn = first_peer.connect(state.clone()).await.unwrap();
    for (i, ckpt_header) in checkpts.iter().enumerate() {
        let ckpt_height =  (i +1) * 1000;
        let start_height = (ckpt_height - 999) as u32;
        println!("checkpt # {} = {:?} ... fetching headers...", ckpt_height, ckpt_header);
        let ckpt_blk_hash = get_block_hash(state.clone(), ckpt_height).await.unwrap();
        let (headers, conn) = 
            fetch_cf_headers_from_peer(
                state.clone(),
                start_height,
                // BlockHash::from_hex("0000000008e647742775a230787d66fdf92c46a48c896bfbc85cdc8acc67e87d").unwrap(), // 999
                ckpt_blk_hash, // 1000
                // BlockHash::from_hex("0000000000000000000a94e99abfcac55d72702dbc967414fdb6f067ca76d969").unwrap(), // 671830
                first_peer.connect(state.clone()).await.unwrap()
            ).await.unwrap();
        assert!(headers.last().unwrap() == ckpt_header);
        for (i, header) in headers.iter().enumerate() {
            println!("hash # {} = {:?}", i, header);
        }
    }
}

#[tokio::test]
async fn test_fetch_cf_headers_from_peer() {
    let state = create_state().unwrap().arc();
    let mut peers= state.clone().get_peers().await.unwrap();
    let mut first_peer = &mut peers[0];
    let (headers, conn) = 
        fetch_cf_headers_from_peer(
            state.clone(),
            1,
            // BlockHash::from_hex("0000000008e647742775a230787d66fdf92c46a48c896bfbc85cdc8acc67e87d").unwrap(), // 999
            BlockHash::from_hex("00000000c937983704a73af28acdec37b049d214adbda81d7e2a3dd146f6ed09").unwrap(), // 1000
            // BlockHash::from_hex("0000000000000000000a94e99abfcac55d72702dbc967414fdb6f067ca76d969").unwrap(), // 671830
            first_peer.connect(state.clone()).await.unwrap()
        ).await.unwrap();
    for (i, header) in headers.iter().enumerate() {
        println!("hash # {} = {:?}", i, header);
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn test_fetch_cf_checkpt_from_peer() {
    let state = create_state().unwrap().arc();
    let mut peers= state.clone().get_peers().await.unwrap();
    let mut first_peer = &mut peers[0];
    let (checkpts, conn) = 
        dbg!(fetch_cf_checkpts_from_peer(
            state.clone(),
            BlockHash::from_hex("0000000000000000000a94e99abfcac55d72702dbc967414fdb6f067ca76d969").unwrap(), // 671830
            first_peer.connect(state.clone()).await.unwrap()
        ).await.unwrap());
    for (i, header) in checkpts.iter().enumerate() {
        println!("checkpt # {} = {:?}", (i +1) * 1000, header);
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn test_fetch_cf_from_peer() {
    let state = create_state().unwrap().arc();
    let mut peers= state.clone().get_peers().await.unwrap();
    let mut first_peer = &mut peers[1];
    let (filters, conn) = 
        dbg!(fetch_filters_from_peer(
            state.clone(),
            0,
            // BlockHash::from_hex("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f").unwrap(), // 0
            // BlockHash::from_hex("00000000000000000007316856900e76b4f7a9139cfbfba89842c8d196cd5f91").unwrap(), // 600000
            // BlockHash::from_hex("00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048").unwrap(), // 1
            // BlockHash::from_hex("000000003b2cfccabec62b1f874ee8e8e3c481a7e78d5188d7777fbbb948f1a0").unwrap(), // 998
            BlockHash::from_hex("0000000008e647742775a230787d66fdf92c46a48c896bfbc85cdc8acc67e87d").unwrap(), // 999
            // BlockHash::from_hex("000000007bc154e0fa7ea32218a72fe2c1bb9f86cf8c9ebf9a715ed27fdb229a").unwrap(), // 100
            first_peer.connect(state.clone()).await.unwrap()
        ).await.unwrap());
    for (i, (hash, filter)) in filters.iter().enumerate() {
        println!("blockfilter # {} = {:?}", i, filter.content.to_hex());
    }

    tokio::time::sleep(Duration::from_secs(10)).await;
}
