use crate::{
    client::{
        GenericRpcMethod, RpcError, RpcRequest, RpcResponse, MISC_ERROR_CODE, PRUNE_ERROR_MESSAGE,
    }, 
    fetch_blocks::fetch_block, 
    rpc_methods::{
        GetBlockCount,
        GetBlockHash,
        // GetBlockHeader, GetBlockHeaderParams, GetBlockResult
    }, 
    undo::BlockUndoCursor
};
use crate::{create_state, state::State};
use std::{collections::HashMap, path::PathBuf, sync::Arc, vec::IntoIter};
// use std::io::Cursor;
// use anyhow::Error;
use anyhow::{anyhow, Context, Error};
use bitcoin::{Block, OutPoint, Script, TxOut, blockdata::undo::{BlockUndo, TxOutUndo, TxUndo}, consensus::{encode::deserialize, Decodable, Encodable}, hash_types::{BlockHash, FilterHash}, hashes::hex::ToHex, util::bip158::BlockFilter};

use serde_json::Value;
use sled::Tree;

fn get_script_for_coin(
    utxo_tree: Tree,
    coin: &OutPoint,
) -> Result<Script, bitcoin::util::bip158::Error> {
    // let db: sled::Db = sled::open("bf").unwrap();
    // let utxo_tree = db.open_tree("utxo").unwrap();
    // println!("getting outpoint {}", coin);
    let mut outpoint_bin = Vec::new();
    coin.consensus_encode(&mut outpoint_bin).unwrap();
    let script_bin = match utxo_tree.get(outpoint_bin) {
        Ok(Some(utxo)) => utxo,
        Ok(None) => return Err(bitcoin::util::bip158::Error::UtxoMissing(coin.clone())),
        Err(e) => {
            return Err(bitcoin::util::bip158::Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e,
            )))
        }
    };
    let script = Script::consensus_decode(&mut std::io::Cursor::new(script_bin)).unwrap();
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
            .await
        {
            Ok(a) => return a.into_result(),
            Err(e) => {
                eprintln!("error getting block hash, retrying: {}", e);
                std::thread::sleep(std::time::Duration::from_secs(1));
                ()
            }
        };
    }
}

async fn fetch_block_by_height(state: Arc<State>, block_height: usize) -> Result<Block, Error> {
    let block_hash = get_block_hash(state.clone(), block_height).await?;
    let block = match fetch_block(
        state.clone(),
        state.clone().get_peers().await.unwrap(),
        block_hash,
    )
    .await
    {
        Ok(Some(block)) => block,
        Ok(None) => return Err(anyhow!("None block returned from fetch_block")),
        Err(e) => return Err(e.into()),
    };
    Ok(block)
}

async fn check_block_utxos(
    state: Arc<State>,
    utxo_tree: Tree,
    block_height: usize,
) -> Result<(), Error> {
    println!("checking block utxos for block {}", block_height);
    let block = fetch_block_by_height(state, block_height).await.unwrap();
    println!("fetched block {}", block.block_hash());
    // utxo_tree.transaction(|tx_db| {
    for tx in &block.txdata {
        for vout in 0..tx.output.len() {
            let outpoint = OutPoint::new(tx.txid(), vout as u32);
            let script = tx.output[vout].script_pubkey.clone();
            if tx.txid().to_string()
                == "03fa5038147f4df74c44541066f79bb76272144bbb56cd60c1cd5b44374494d9"
            {
                println!("inserting outpoint {} and script {}", outpoint, script);
            }
            let mut outpoint_bin = Vec::new();
            outpoint.consensus_encode(&mut outpoint_bin).unwrap();
            let mut script_bin = Vec::new();
            script.consensus_encode(&mut script_bin).unwrap();
            if utxo_tree.contains_key(&outpoint_bin).unwrap() {
                println!("outpoint found: {}", outpoint);
            } else {
                println!("outpoint missing, inserting: {}", outpoint);
                // utxo_tree.insert(outpoint_bin, script_bin);
            }
        }
    }
    // Ok(())
    // })?;
    Ok(())
}

pub async fn build_filter_index_and_utxo_set(state: Arc<State>) -> Result<(), Error> {
    // let block_undo_cursor = BlockUndoCursor::new();
    let result = state
        .rpc_client
        .call(&RpcRequest {
            id: None,
            method: GetBlockCount,
            params: [(); 0],
        })
        .await?
        .into_result();
    let db: sled::Db = sled::open("bf").unwrap();
    let utxo_tree = db.open_tree("utxo").unwrap();
    let bf_tree = db.open_tree("bf").unwrap();
    // for i in 0..=count {
    let count = result.unwrap();
    println!("count={}", count);
    for i in 0..=count {
        // for i in 0..=count {
        // if i % 1000 == 0 || i > 133999 {
        if i % 1000 == 0 {
            // bf_tree.flush();
            // utxo_tree.flush();
            println!("requesting block hash for height {:?}", i);
        }

        // loop {
        let block = fetch_block_by_height(state.clone(), i).await?;
        // {
        //     Ok(block) => block,
        //     // Ok(None) => return Err(anyhow!("None block returned from fetch_block")),
        //     Err(e) => {
        //         println!("error on block height {}, retrying", i);
        //         println!("error: {}", e);
        //         // Err(e.into())
        //         ()
        //     },
        // };
        // }

        utxo_tree.transaction(|utxo_tree| {
            use sled::transaction::ConflictableTransactionError::Abort;

            // insert new utxos
            for tx in &block.txdata {
                for vout in 0..tx.output.len() {
                    let outpoint = OutPoint::new(tx.txid(), vout as u32);
                    let script = &tx.output[vout].script_pubkey;
                    // if tx.txid().to_string() == "03fa5038147f4df74c44541066f79bb76272144bbb56cd60c1cd5b44374494d9" {
                    // if i > 134334 {
                    //     // println!("inserting outpoint {} and script {}", outpoint, script);
                    //     println!("inserting outpoint {}", outpoint);
                    // }
                    let mut outpoint_bin = Vec::new();
                    outpoint
                        .consensus_encode(&mut outpoint_bin)
                        .map_err(Abort)?;
                    let mut script_bin = Vec::new();
                    script.consensus_encode(&mut script_bin).map_err(Abort)?;
                    utxo_tree.insert(outpoint_bin, script_bin)?;
                }
            }
            Ok(())
        })?;

        // calculate and store filter
        let block_filter =
            BlockFilter::new_script_filter(
                &block, 
                |o| get_script_for_coin(utxo_tree.clone(), o)
            ).unwrap();
        // println!("filter = {:?}", block_filter.content.to_hex());
        bf_tree.insert(block.block_hash(), block_filter.content.as_slice())?;

        utxo_tree.transaction(|utxo_tree| {
            use sled::transaction::ConflictableTransactionError::Abort;
            for tx in &block.txdata {
                if !tx.is_coin_base() {
                    if tx.input.len() > 0 {
                        for vin in 0..tx.input.len() {
                            // let outpoint = OutPoint::new(tx.txid(), vout as u32);
                            let outpoint = &tx.input[vin].previous_output;
                            // if tx.txid().to_string() == "03fa5038147f4df74c44541066f79bb76272144bbb56cd60c1cd5b44374494d9" {
                            // println!("deleting outpoint {} in block hash: {}", outpoint, block.block_hash());
                            // }
                            let mut outpoint_bin = Vec::new();
                            outpoint
                                .consensus_encode(&mut outpoint_bin)
                                .map_err(Abort)?;
                                utxo_tree.remove(outpoint_bin)?;
                        }
                    }
                }
            }
            Ok(())
        })?;
    }
    // println!("count = {:?}", count);
    Ok(())
}

fn get_script_from_undo<I: Iterator<Item = TxOutUndo>>(iter: &mut I, o: &OutPoint) -> Result<Script, bitcoin::util::bip158::Error> {
    // println!("contents of iterator: {:?}", iter.next());
    if let Some(txout_undo) = iter.next() {
        Ok(txout_undo.script_pubkey.0)
    }
    else {
        println!("called get_script_from_undo for coin {}", o);
        panic!();
    }
}

pub async fn build_filter_index_from_undo_data(state: Arc<State>) -> Result<(), Error> {
    // let block_undo_cursor = BlockUndoCursor::new();
    let result = state
        .rpc_client
        .call(&RpcRequest {
            id: None,
            method: GetBlockCount,
            params: [(); 0],
        })
        .await?
        .into_result();
    let db: sled::Db = sled::open("bf").unwrap();
    let bf_tree = db.open_tree("bf").unwrap();
    // for i in 0..=count {
    let count = result.unwrap();
    println!("count={}", count);
    // let mut c = BlockUndoCursor::new(PathBuf::from("/home/cguida/.bitcoin/blocks"), 0, 19502380).await;
    let mut c = BlockUndoCursor::new(PathBuf::from("/home/cguida/.bitcoin/blocks"), 0, 0).await;
    for i in 0..=count {
        // for i in 0..=count {
        println!("requesting block hash for height {:?}", i);

        let block = fetch_block_by_height(state.clone(), i).await?;
        let flat_map_fn = |tx_undo: TxUndo| tx_undo.output_undo;
        let block_undo = if i > 0 {
            c.next().await.unwrap().unwrap()
        } else {
            BlockUndo{txdata_undo: Vec::<TxUndo>::new()}
        };
        assert_eq!(block_undo.txdata_undo.len(), block.txdata.len() - 1);
        let mut block_output_iter = block_undo.txdata_undo.into_iter().flat_map(flat_map_fn);
        
        // if i > 0 {
        //     let block_undo = 
        //     println!("found unwrapped block {:?}", block_undo);
        //     block_undo.txdata_undo.into_iter().flat_map(flat_map_fn)
        // } else {
        //     let block_undo = 
        //     Vec::<TxUndo>::new().into_iter().flat_map(flat_map_fn)
        // };



        // println!("iterator contents: {:?}", block_output_iter.next());

        // calculate and store filter
        let block_filter =
            BlockFilter::new_script_filter(
                &block, 
                |o| get_script_from_undo(&mut block_output_iter, o)
            ).unwrap();
        if i % 1000 == 0 {
            println!("filter = {:?}", block_filter.content.to_hex());
        }
        bf_tree.insert(block.block_hash(), block_filter.content.as_slice())?;

    }
    // println!("count = {:?}", count);
    Ok(())
}



#[tokio::test]
async fn test_undo() {
    let state = create_state::create_state().unwrap().arc();
    build_filter_index_from_undo_data(state).await;
}


#[tokio::test]
async fn test_utxo() {
    let state = create_state::create_state().unwrap().arc();
    build_filter_index_and_utxo_set(state).await;
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
    let db: sled::Db = sled::open("bf").unwrap();
    let bf_tree = db.open_tree("bf").unwrap();
    // print single filter
    let block_hash: BlockHash = "00000000000007003fb47df9f520e292413501053b1e337a2349c093ffc6667e"
        .parse()
        .unwrap();
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

// fn db_test() {
//     let filter_index = initialize_filter_index().unwrap();
//     println!("filter_index = {:?}\n", filter_index);

//     let db: sled::Db = sled::open("bfd").unwrap();

//     // let block_hash: BlockHash = "00000000fd3ceb2404ff07a785c7fdcc76619edc8ed61bd25134eaa22084366a".parse().unwrap();
//     // let block_filter = filter_index.get(&block_hash).unwrap().clone();

//     for (block_hash, block_filter) in filter_index.iter() {
//         println!("block_hash {:?} block_filter {:?}", block_hash, block_filter.content.as_slice());
//         if db.contains_key(block_hash).unwrap() {
//             println!("key already exists. skipping");
//         }  else {
//             println!("key does not exist, inserting");
//             db.insert(block_hash, block_filter.content.as_slice());
//             let block_filter_content_from_db = &db.get(block_hash).unwrap().unwrap();
//             println!("block_filter_content_from_db = {:?}", block_filter_content_from_db);
//             let block_filter_from_db = BlockFilter::new(&block_filter_content_from_db);
//             println!("block_filter_from_db = {:?}", block_filter_from_db);
//         }
//     }

//     for result in db.iter() {
//         let (block_hash_bin, block_filter_bin) = result.unwrap();
//         let block_hash = BlockHash::consensus_decode(&mut std::io::Cursor::new(
//             block_hash_bin
//         )).unwrap();
//         println!("block_hash {:?} block_filter {:?}", block_hash, block_filter_bin.to_hex());
//     }
// }
