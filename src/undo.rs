use std::{fmt, io::SeekFrom, usize};
use std::error::{self, Error};
use hex::{FromHex, decode, decode_to_slice};
use serde_json::de::Read;
use std::io::{self};
use std::path::{Path, PathBuf};
use tokio::{fs::File, io::{AsyncReadExt, BufReader, ErrorKind}};
use bitcoin::{blockdata::undo::{BlockUndo, TxUndo}, consensus::{Decodable, Encodable, ReadExt, encode::VarInt}, hashes::{sha256d, Hash}};

use crate::index::{BlockIndexRecord, get_block_index};


const MAGIC_BYTES: [u8; 4] = [249, 190, 180, 217];
const MAGIC_BYTES_INT: u32 = 3652501241;

pub struct BlockUndoCursor {
    base_path: PathBuf,
    index: Vec<BlockIndexRecord>,
    height: usize,
}

impl BlockUndoCursor {
    pub async fn new(base_path: PathBuf) -> Self {
        let base_path_clone = base_path.clone();
        println!("about to get_block_index");
        let index = get_block_index(base_path.join("index").as_path()).unwrap();
        println!("just called get_block_index");
        BlockUndoCursor {
            base_path: base_path_clone,
            index: index,
            height: 0,
        }

    }

    pub async fn next(&mut self) -> Option<Result<BlockUndo, anyhow::Error>> {
        if self.height == 0 {
            self.height += 1;
            return Some(Ok(BlockUndo{txdata_undo: Vec::<TxUndo>::new()}));
        }
        let blk_index = &self.index[self.height];
        // println!("blkindex = {:?}", blk_index);
        let mut file_handle = File::open(self.base_path.join(format!("rev{:05}.dat", blk_index.n_file))).await.unwrap();
        // println!("blkindex.n_data_pos_undo = {}", blk_index.n_data_pos_undo);
        file_handle.seek(SeekFrom::Start(blk_index.n_data_pos_undo - 4)).await;
        // println!("reading file {}, offset {}", self.current_file_index, self.current_file_offset);
        let mut f = BufReader::new(file_handle);
        let block_size = f.read_u32_le().await.unwrap();
        // f.read_block_undo(block_size, version_id);
        // read BlockUndo
        let mut block_buf = Vec::with_capacity(block_size as usize);
        // tokio::io::copy(&mut self.current_file_reader.take(len as u64), &mut block_buf).await.unwrap();
        let block_len = tokio::io::copy(&mut (&mut f).take(block_size as u64), &mut block_buf).await.unwrap();
        // self.current_file_offset += block_len as usize;
        // let block_len = self.current_file_reader.read_exact(&mut block_buf).await.unwrap();
        // println!("block_buf {:?} length {} found", block_buf, block_len);
        
        let mut block_cursor = std::io::Cursor::new(block_buf);
        
        // parse BlockUndo
        let block_undo = BlockUndo::consensus_decode(&mut block_cursor).unwrap();
        // println!("block_undo parsed: {:?}", block_undo);

        self.height += 1;

        // read 32-byte checksum to arrive at next block
        // let mut checksum_buf = Vec::with_capacity(32);
        // let checksum_len= tokio::io::copy(&mut (&mut self.current_file_reader).take(32), &mut checksum_buf).await.unwrap();
        // self.current_file_offset += checksum_len as usize;
        // self.current_block_index += 1;
        // // let checksum_len = self.current_file_reader.read_exact(&mut checksum_buf).await.unwrap();
        // println!("checksum_buf {:?} found, length {}", checksum_buf, checksum_len);
        Some(Ok(block_undo))
    }
}

#[tokio::test]
async fn test_undo() {
    let count: i32 = 400;
    let mut c = BlockUndoCursor::new(PathBuf::from("/home/cguida/.bitcoin/testnet3/blocks")).await;
    for i in 0..=count {
        let block_undo = c.next().await.unwrap().unwrap();
        // if i < 200 {
            println!("block number {} block_undo {:?}", i, block_undo);
            // println!("block number {}", i);
        // }


    }
}

