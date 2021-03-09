use crate::{client::{
        GenericRpcMethod, MISC_ERROR_CODE, PRUNE_ERROR_MESSAGE, RpcError, 
        RpcRequest, 
        RpcResponse
    }, fetch_blocks::{fetch_block, fetch_filters_from_self}, rpc_methods::{
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

async fn get_block_hashes(state: Arc<State>, start_height: usize, end_height: usize) -> Result<Vec<BlockHash>, RpcError> {
    let mut reqs = Vec::<RpcRequest<GetBlockHash>>::new();
    for i in start_height..=end_height {
        reqs.push(RpcRequest {
            id: Some(i.into()),
            method: GetBlockHash,
            params: (i,),
        });
    }
    println!("reqs = {:?}", reqs);
    loop {
        let result = match state
        .rpc_client
        .call_batch(&reqs)
        .await {
            Ok(hash_responses) => {
                println!("hash_responses = {:?}", hash_responses);
                let mut hashes = Vec::<BlockHash>::new();
                for hash_response in hash_responses {
                    hashes.push(
                        hash_response.into_result().unwrap()
                    );
                }
                // println!("hahes={:?}", hashes);
                return Ok(hashes)
            },
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
    Ok(block)
}

pub async fn find_descriptor_blocks_from_rpc_filters(state: Arc<State>) -> Result<(), Error> {
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
    // let db: sled::Db = sled::open("bf").unwrap();
    // let bf_tree = db.open_tree("bf").unwrap();
    // let count = result.unwrap(); 
    let count = 123; 
    println!("count={}", count);

    // fetch all filters from rpc
    // for batch in 0..=(count / 1000) {
    // use sled::transaction::ConflictableTransactionError::Abort;
    let batch_size = 10;
    let num_batches = count / batch_size;
    for batch in 0..=num_batches {
        // let filter_batch = Vec::<(&[u8], &[u8])>::new();
        // let mut db_batch = sled::Batch::default();
        let start_height = batch * batch_size;
        let end_height = match batch == num_batches {
            true => count,
            false => start_height + batch_size - 1
        };
        let hashes = get_block_hashes(state.clone(), start_height, end_height).await?;

        // for i in start_height..end_height {
        let filters = match fetch_filters_from_self(
            &state.clone(),
            hashes.clone(),
        )
        .await?
        {
            Some(filter) => {
                filter
            },
            None => {
                println!("None filter on fetch_filter_from_self for batch with hash 0 = {:?}", hashes[0]);
                return Err(anyhow!("None filter returned from fetch_filter_from_self"))
            },
        };
    
        // println!("inserting height {} {:?} filter {:?}", i, hash, filter.content.to_hex());
        // if i % 1000 == 0 {
            // bf_tree.flush_async().await?;
        // }
        let mut filter_batch = Vec::<(&[u8], &[u8])>::new();
        println!("inserting starting height {} {:?} filter {:?}", start_height, &hashes[0], filters[0].content.to_hex());
        println!("inserting ending height {} {:?} filter {:?}", end_height, &hashes.last().unwrap(), filters.last().unwrap().content.to_hex());
        for i in 0..hashes.len() {
            let mut hash_bin = Vec::new();
            hashes[i]
                .consensus_encode(&mut hash_bin).unwrap();
            // bf_tree.insert(hash_bin, filters.content.as_slice())?;
            // db_batch.insert(hash_bin, filters[i].content.as_slice());
        }
        // db.apply_batch(db_batch);
        
    }
    Ok(())
}

#[tokio::test]
async fn test_peers() {
    let start_time= std::time::SystemTime::now();
    println!("starting at {:?}", start_time);
    let state = create_state::create_state().unwrap().arc();
    find_descriptor_blocks_from_rpc_filters(state).await;
    println!("finished at {:?}", start_time.elapsed().unwrap());
    std::thread::sleep(std::time::Duration::from_secs(1));
    // let db: sled::Db = sled::open("bf").unwrap();
    // let utxo_tree = db.open_tree("utxo").unwrap();
    // let bf_tree = db.open_tree("bf").unwrap();
}

// #[tokio::test]
// async fn check_utxos_for_height() {
//     let state = create_state::create_state().unwrap().arc();
//     // build_filter_index_and_utxo_set(state).await;
//     println!("opening bf");
//     let db: sled::Db = sled::open("bf").unwrap();
//     println!("opening utxo tree");
//     let utxo_tree = db.open_tree("utxo").unwrap();
//     // check utxos for block height
//     let block_height = 278234;
//     println!("about to check utxos for block {}", block_height);
//     check_block_utxos(state, utxo_tree, block_height).await;
// }

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


// #[tokio::test]
// async fn fetch_and_verify_filter_header_chain() {
//     let state = create_state::create_state().unwrap().arc();
//     let mut peers= state.clone().get_peers().await.unwrap();
//     let checkpts = 
//         fetch_cf_checkpts_from_peers(
//             state.clone(),
//             peers,
//             BlockHash::from_hex("0000000000000000000a94e99abfcac55d72702dbc967414fdb6f067ca76d969").unwrap(), // 671830
//         ).await.unwrap();
//     let mut all_headers = Vec::<FilterHeader>::new();

//     // verify the filter header chain against the checkpoints
//     for (i, ckpt_header) in checkpts.iter().enumerate() {
//         let ckpt_height =  (i +1) * 1000;
//         let start_height = (ckpt_height - 999) as u32;
//         println!("checkpt # {} = {:?} ... fetching headers...", ckpt_height, ckpt_header);
//         let ckpt_blk_hash = get_block_hash(state.clone(), ckpt_height).await.unwrap();
//         let headers = 
//             fetch_cf_headers_from_peers(
//                 state.clone(),
//                 state.clone().get_peers().await.unwrap(),
//                 start_height,
//                 ckpt_blk_hash,
//             ).await.unwrap();
//         assert!(headers.last().unwrap() == ckpt_header);
//         for (i, header) in headers.iter().enumerate() {
//             println!("hash # {} = {:?}", i, header);
//             all_headers.push(*header);
//         }
//     }
// }
