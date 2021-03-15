use crate::{client::{
        GenericRpcMethod, MISC_ERROR_CODE, PRUNE_ERROR_MESSAGE, RpcError, 
        RpcRequest, 
        RpcResponse
    }, fetch_blocks::{fetch_block, fetch_filters_from_self}, rpc_methods::{
        GetBlockCount, GetBlockHash, DeriveAddresses, DeriveAddressesParams,
    }};
use crate::{
    state::State, 
    create_state
};
use std::{collections::HashMap, hash::Hash, str::FromStr, sync::Arc, time::SystemTime};
use anyhow::{anyhow, Context, Error};
use bitcoin::{
    Address, Block, FilterHeader, OutPoint, Script, Transaction, Txid, 
    consensus::{Encodable, encode::deserialize, Decodable}, 
    hash_types::{BlockHash, FilterHash}, 
    hashes::hex::{FromHex, ToHex}, util::bip158::{BlockFilter,}
};

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
    loop {
        let result = match state
        .rpc_client
        .call_batch(&reqs)
        .await {
            Ok(hash_responses) => {
                let mut hashes = Vec::<BlockHash>::new();
                for hash_response in hash_responses {
                    hashes.push(
                        hash_response.into_result().unwrap()
                    );
                }
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

pub async fn derive_addresses(state: Arc<State>, descriptor_string: String) -> Result<Vec<Address>, Error> {
    let addresses = state
      .rpc_client
      .call(&RpcRequest {
          id: None,
          method: DeriveAddresses,
          params: DeriveAddressesParams {
              0: descriptor_string,
              1: None,
            //   1: Some([0, 1000].into()),
          }
      })
      .await?
      .into_result().unwrap()
    ;
    Ok(addresses)
}

pub async fn find_blocks_from_rpc_filters(state: Arc<State>, descriptor_string: String) -> Result<Vec::<BlockHash>, Error> {
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
    let count = result.unwrap(); 
    println!("count={}", count);
    let batch_size = 1000;
    let num_batches = count / batch_size;
    let addresses = derive_addresses(state.clone(), descriptor_string).await.unwrap();
    println!("addresses = {:?}", addresses);
    let scripts: Vec::<Script> = addresses.iter().map(|a| a.payload.script_pubkey()).collect();
    let mut match_count: i32 = 0;
    let mut relevant_hashes = Vec::<BlockHash>::new();
    for batch in 0..=num_batches {
        let start_height = batch * batch_size;
        let end_height = match batch == num_batches {
            true => count,
            false => start_height + batch_size - 1
        };
        let hashes = get_block_hashes(state.clone(), start_height, end_height).await?;
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
    
        println!("inserting starting height {} {:?}", start_height, &hashes[0]);
        // println!("inserting ending height {} {:?} filter {:?}", end_height, &hashes.last().unwrap(), filters.last().unwrap().content.to_hex());
        for i in 0..hashes.len() {
            let query = &scripts;
            // println!("looking for script {:?} in hash {:?}", script, hashes[i]);
            if filters[i].match_any(
                &hashes[i], 
                &mut query.iter().map(
                    |s| s.as_bytes())
                ).unwrap()
            {
                println!("match found in block with hash: {:?}", hashes[i]);
                relevant_hashes.push(hashes[i]);
                match_count += 1;
            };
        }
    }
    println!("{} blocks with relevant transations", match_count);
    Ok(relevant_hashes)
}


pub async fn scan_blocks_for_txns(state: Arc<State>, relevant_hashes: Vec<BlockHash>, search_scripts: Vec<Script>) -> Result<Vec::<Transaction>, Error> {
    let mut relevant_txns = Vec::<Transaction>::new();
    for block_hash in relevant_hashes {
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
        for txn in block.txdata {
            let mut txn_match = false; 
            for tx_out in &txn.output {
                for script in &search_scripts {
                    if &tx_out.script_pubkey == script {
                        txn_match = true;
                        println!("match found! tx hash: {:?}", txn.txid());
                    }
                }
            }
            if txn_match {
                relevant_txns.push(txn);
            }
        }
    }
    Ok(relevant_txns)
}

#[tokio::test]
async fn test_scan() {
    let start_time= std::time::SystemTime::now();
    println!("starting at {:?}", start_time);
    let state = create_state::create_state().unwrap().arc();
    let descriptor_string: String = "addr(bc1q7nape377wsk3xh4g9st6mu9mnn6lf7uremmetj)#8hhmm2jt".into();
    let addresses = derive_addresses(state.clone(), descriptor_string.clone()).await.unwrap();
    let relevant_hashes = match find_blocks_from_rpc_filters(state.clone(), descriptor_string.clone()).await {
        Ok(rbh) => rbh,
        Err(e) => {
            // eprintln!("error scanning block filters {}", e);
            panic!("error scanning block filters {}", e);
        },
    };

    let search_scripts = addresses.iter().map(|a| a.payload.script_pubkey()).collect();
    let relevant_txns = scan_blocks_for_txns(state, relevant_hashes, search_scripts).await.unwrap();
    let relevant_txids: Vec<Txid> = relevant_txns.iter().map(|t| t.txid()).collect();
    println!("found {} relevant txns. hashes: {:?}", relevant_txns.len(), relevant_txids);
    println!("finished at {:?}", start_time.elapsed().unwrap());
    std::thread::sleep(std::time::Duration::from_secs(1));
}

#[tokio::test]
async fn scripts_from_addresses() {
    let address = Address::from_str("bc1q7nape377wsk3xh4g9st6mu9mnn6lf7uremmetj").unwrap();
    println!("output script = {:?}", address.payload.script_pubkey());
    let address = Address::from_str("1PLKVvWUFGCYRNDzUQFXn1ErJ7bXbinzrM").unwrap();
    println!("output script = {:?}", address.payload.script_pubkey());
}
