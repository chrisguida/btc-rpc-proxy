use crate::{client::{
        GenericRpcMethod, MISC_ERROR_CODE, PRUNE_ERROR_MESSAGE, RpcError, 
        RpcRequest, 
        RpcResponse
    }, fetch_blocks::{fetch_block, fetch_filters_from_peers, fetch_cf_checkpts_from_peers, fetch_cf_headers_from_peers}, rpc_methods::{
        GetBlockCount, GetBlockHash, 
        // GetBlockHeader, GetBlockHeaderParams, GetBlockResult
    }};
use crate::{
    state::State, 
    create_state
};
use std::{collections::HashMap, sync::Arc, time::SystemTime};
use anyhow::{anyhow, Context, Error};
use bitcoin::{Block, FilterHeader, OutPoint, Script, consensus::{Encodable, encode::deserialize, Decodable}, hash_types::{BlockHash, FilterHash}, hashes::hex::{FromHex, ToHex}, util::bip158::{
        BlockFilter, 
    }};

use enum_future::futures::stream::Filter;
use serde_json::Value;
use sled::Tree;

fn get_script_for_coin (utxo_tree: Tree, coin: &OutPoint) -> Result<Script, bitcoin::util::bip158::Error> {
    // let db: sled::Db = sled::open("bf").unwrap();
    // let utxo_tree = db.open_tree("utxo").unwrap();
    // println!("getting outpoint {}", coin);
    let mut outpoint_bin = Vec::new();
        coin.consensus_encode(
        &mut outpoint_bin, 
    ).unwrap();
    let script_bin = match utxo_tree.get(outpoint_bin) {
        Ok(Some(utxo)) => utxo,
        Ok(None) => return Err(bitcoin::util::bip158::Error::UtxoMissing(coin.clone())),
        Err(e) => return Err(bitcoin::util::bip158::Error::Io(std::io::Error::new(std::io::ErrorKind::Other, e))),
    };
    let script = Script::consensus_decode(&mut std::io::Cursor::new(
        script_bin
    )).unwrap();
    // Ok(&utxo_tree.get(outpoint_bin).unwrap().unwrap())
    Ok(script)
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

async fn fetch_block_by_height(state:Arc<State>, block_height: usize) -> Result<Block, Error> {
    let block_hash = get_block_hash(state.clone(), block_height).await?;
    let block = match fetch_block(
        state.clone(), 
        state.clone().get_peers().await.unwrap(), 
        block_hash
    )
    .await 
    {
        Ok(Some(block)) => {
            if block_height == 134335 {
                println!("Some block on fetch_block for hash = {}", block_hash);
            }
            block
        },
        Ok(None) => {
            println!("None block on fetch_block for hash = {}", block_hash);
            return Err(anyhow!("None block returned from fetch_block"))
        },
        Err(e) => {
            println!("Error on fetch_block for hash = {}", block_hash);
            return Err(e.into())
        },
    };
    if block_height == 134335 {
        println!("block height 134335 successful!");
    }
    // let block = result.await.unwrap().unwrap();
    Ok(block)
}

async fn check_block_utxos(state: Arc<State>, utxo_tree: Tree, block_height: usize) -> Result<(), Error> {
    let block = fetch_block_by_height(state, block_height).await.unwrap();
    for tx in &block.txdata {
        for vout in 0..tx.output.len() {
            let outpoint = OutPoint::new(tx.txid(), vout as u32);
            let script = tx.output[vout].script_pubkey.clone();
            if tx.txid().to_string() == "03fa5038147f4df74c44541066f79bb76272144bbb56cd60c1cd5b44374494d9" {
                println!("inserting outpoint {} and script {}", outpoint, script);
            }
            let mut outpoint_bin = Vec::new();
            outpoint.consensus_encode(
                &mut outpoint_bin, 
            ).unwrap();
            let mut script_bin = Vec::new();
            script.consensus_encode(
                &mut script_bin, 
            ).unwrap();
            if utxo_tree.contains_key(&outpoint_bin).unwrap() {
                println!("outpoint found: {}", outpoint);
            } else {
                println!("outpoint missing, inserting: {}", outpoint);
                // utxo_tree.insert(outpoint_bin, script_bin);
            }
        }
    }
    Ok(())
}

pub async fn build_filter_index_from_peers(state: Arc<State>) -> Result<(), Error> {
    println!("getting block count");
    let result = state
        .rpc_client
        .call(&RpcRequest {
            id: None,
            method: GetBlockCount,
            params: [(); 0],
        })
        .await?
        .into_result()
        ;
    let db: sled::Db = sled::open("bf").unwrap();
    let bf_tree = db.open_tree("bf").unwrap();
    let fh_tree = db.open_tree("fh").unwrap();
    // for i in 0..=count {
    let count = result.unwrap(); 
    println!("count={}", count);

    // fetch checkpoints
    let checkpts = 
        fetch_cf_checkpts_from_peers(
            state.clone(),
            state.clone().get_peers().await.unwrap(),
            get_block_hash(state.clone(), count).await.unwrap(),
        ).await.unwrap();
        
    // fetch and verify filter header chain against checkpoints
    let mut all_headers = Vec::<FilterHeader>::new();
    for (i, ckpt_header) in checkpts.iter().enumerate() {
        let ckpt_height =  (i +1) * 1000;
        let start_height: u32;
        // get
        if i == 0 {
            start_height = 0
        } else {
            start_height = (ckpt_height - 999) as u32;
        }
        println!("checkpt # {} = {:?} ... fetching headers...", ckpt_height, ckpt_header);
        let ckpt_blk_hash = get_block_hash(state.clone(), ckpt_height).await.unwrap();
        let headers = 
            fetch_cf_headers_from_peers(
                state.clone(),
                state.clone().get_peers().await.unwrap(),
                start_height,
                ckpt_blk_hash,
            ).await.unwrap();
        assert!(headers.last().unwrap() == ckpt_header);
        for (i, header) in headers.iter().enumerate() {
            // println!("hash # {} = {:?}", i, header);
            all_headers.push(*header);
            let mut header_bin = Vec::new();
            header.consensus_encode(
                &mut header_bin, 
            ).unwrap();
            fh_tree.insert(i.to_le_bytes(), header_bin)?;
        }
    }

    // fetch and verify filters against header chain
    let mut previous_filter_header = FilterHeader::from_hex("0000000000000000000000000000000000000000000000000000000000000000").unwrap();
    for batch in 0..=(count / 1000) {
        // if batch % 1000 == 0 {
            let start_height = batch * 1000;
            let stop_height = start_height + 999;
            let stop_hash = get_block_hash(state.clone(), stop_height).await?;
            println!("getting filters start {:?} stop {:?} stop_hash {:?}", start_height, stop_height, stop_hash);
            let filters = match fetch_filters_from_peers(
                state.clone(), 
                state.clone().get_peers().await.unwrap(), 
                start_height as u32,
                stop_hash
            )
            .await 
            {
                Some(filters) => {
                    filters
                },
                None => {
                    println!("None filter on fetch_filters_from_peers for hash = {}", stop_hash);
                    return Err(anyhow!("None filter returned from fetch_filters_from_peers"))
                },
            };
        // }
        
        // bf_tree.transaction(|bf_tree| {
            use sled::transaction::ConflictableTransactionError::Abort;

            for (i, (hash, filter) )in filters.iter().enumerate() {
                let block_height = start_height + i;
                let header_from_peers = all_headers[block_height];
                let header_from_filter_calc = filter.filter_header(&previous_filter_header);
                // println!("comparing headers {} and {}", header_from_peers, header_from_filter_calc);
                assert!(header_from_peers == header_from_filter_calc);
                if i == 0 {
                    println!("inserting height {} {:?} filter {:?}", block_height, hash, filter.content.to_hex());
                }
                let mut hash_bin = Vec::new();
                hash
                    .consensus_encode(&mut hash_bin)
                    .map_err(Abort)?;
                bf_tree.insert(hash_bin, filter.content.as_slice())?;
                previous_filter_header = header_from_peers;
            }
            // Ok(())
        // })?;
    }
    // println!("count = {:?}", count);
    Ok(())
}

#[tokio::test]
async fn test_peers() {
    let start_time= std::time::SystemTime::now();
    println!("starting at {:?}", start_time);
    let state = create_state::create_state().unwrap().arc();
    build_filter_index_from_peers(state).await;
    println!("finished at {:?}", start_time.elapsed().unwrap());
    // let db: sled::Db = sled::open("bf").unwrap();
    // let utxo_tree = db.open_tree("utxo").unwrap();
    // let bf_tree = db.open_tree("bf").unwrap();
}

#[tokio::test]
async fn check_utxos_for_height() {
    let state = create_state::create_state().unwrap().arc();
    // build_filter_index_and_utxo_set(state).await;
    println!("opening bf");
    let db: sled::Db = sled::open("bf").unwrap();
    println!("opening utxo tree");
    let utxo_tree = db.open_tree("utxo").unwrap();
    // check utxos for block height
    let block_height = 278234;
    println!("about to check utxos for block {}", block_height);
    check_block_utxos(state, utxo_tree, block_height).await;
}

#[tokio::test]
async fn print_all_utxos() {
    println!("opening bf");
    let db: sled::Db = sled::open("bf").unwrap();
    println!("opening utxo tree");
    let utxo_tree = db.open_tree("utxo").unwrap();
    //print all utxos
    let mut iter = utxo_tree.iter();
    // for i in 0..10{
    // let result = iter[i].unwrap();
    for result in utxo_tree.iter() {
        let (outpoint_bin, script_bin) = result.unwrap();
        let outpoint = OutPoint::consensus_decode(&mut std::io::Cursor::new(outpoint_bin)).unwrap();
        let script = Script::consensus_decode(&mut std::io::Cursor::new(script_bin)).unwrap();
        println!("outpoint {} script {}", outpoint, script);
    }
}

#[tokio::test]
async fn print_single_filter() {
    println!("opening db");
    let db: sled::Db = sled::open("bf").unwrap();
    println!("opening bf_tree");
    let bf_tree = db.open_tree("bf").unwrap();
    // print single filter
    println!("getting block hash");
    let block_hash: BlockHash = "0000000000000000000aa87584856393a3cf61948c5ecc235f53922423d0f766"
    .parse()
    .unwrap();
    println!("getting filter");
    let block_filter_bin = bf_tree.get(&block_hash).unwrap().unwrap();
    // if block_filter_bin.is_none() {
    //     println!("Found None value in block");
    // }
    println!(
        "block_hash {:?} block_filter {:?}",
        block_hash,
        block_filter_bin.to_hex()
    );
}

#[tokio::test]
async fn print_all_filters() {
    let db: sled::Db = sled::open("bf").unwrap();
    let bf_tree = db.open_tree("bf").unwrap();
    // // print all filters
    for result in bf_tree.iter() {
        // iter = bf_tree.iter();
        // for i in 0..10 {
        //     let result = iter[i].unwrap();
        // // }
        let (block_hash_bin, block_filter_bin) = result.unwrap();
        let block_hash =
            BlockHash::consensus_decode(&mut std::io::Cursor::new(block_hash_bin)).unwrap();
        if block_hash.to_string()
            == "0000000099c744455f58e6c6e98b671e1bf7f37346bfd4cf5d0274ad8ee660cb"
        {
            println!(
                "block_hash {:?} block_filter {:?}",
                block_hash,
                block_filter_bin.to_hex()
            );
        }
    }
}


#[tokio::test]
async fn fetch_and_verify_filter_header_chain() {
    let state = create_state::create_state().unwrap().arc();
    let mut peers= state.clone().get_peers().await.unwrap();
    let checkpts = 
        fetch_cf_checkpts_from_peers(
            state.clone(),
            peers,
            BlockHash::from_hex("0000000000000000000a94e99abfcac55d72702dbc967414fdb6f067ca76d969").unwrap(), // 671830
        ).await.unwrap();
    let mut all_headers = Vec::<FilterHeader>::new();

    // verify the filter header chain against the checkpoints
    for (i, ckpt_header) in checkpts.iter().enumerate() {
        let ckpt_height =  (i +1) * 1000;
        let start_height = (ckpt_height - 999) as u32;
        println!("checkpt # {} = {:?} ... fetching headers...", ckpt_height, ckpt_header);
        let ckpt_blk_hash = get_block_hash(state.clone(), ckpt_height).await.unwrap();
        let headers = 
            fetch_cf_headers_from_peers(
                state.clone(),
                state.clone().get_peers().await.unwrap(),
                start_height,
                ckpt_blk_hash,
            ).await.unwrap();
        assert!(headers.last().unwrap() == ckpt_header);
        for (i, header) in headers.iter().enumerate() {
            println!("hash # {} = {:?}", i, header);
            all_headers.push(*header);
        }
    }
}
