use anyhow::Error;
use std::convert::TryInto;
use std::fmt;
use std::io::Cursor;
use std::path::Path;

use bitcoin::consensus::ReadExt;
// use byteorder::ReadBytesExt;
use rusty_leveldb::{LdbIterator, Options, DB};

// use crate::errors::OpResult;

const BLOCK_VALID_CHAIN: usize = 4;
const BLOCK_HAVE_DATA: usize = 8;

/// https://bitcoin.stackexchange.com/questions/28168/what-are-the-keys-used-in-the-blockchain-leveldb-ie-what-are-the-keyvalue-pair
pub struct BlockIndexRecord {
    pub block_hash: [u8; 32],
    version: usize,
    height: usize,
    status: usize,
    n_tx: usize,
    pub n_file: usize,
    pub n_data_pos: u64,
    pub n_data_pos_undo: u64,
}

impl BlockIndexRecord {
    fn from(key: &[u8], values: &[u8]) -> Result<Self, Error> {
        let mut reader = Cursor::new(values);

        let block_hash: [u8; 32] = key.try_into().expect("leveldb: malformed blockhash");
        let version = read_varint(&mut reader)?;
        let height = read_varint(&mut reader)?;
        let status = read_varint(&mut reader)?;
        let n_tx = read_varint(&mut reader)?;
        let n_file = read_varint(&mut reader)?;
        let n_data_pos = read_varint(&mut reader)? as u64;
        let n_data_pos_undo = read_varint(&mut reader)? as u64;

        Ok(BlockIndexRecord {
            block_hash,
            version,
            height,
            status,
            n_tx,
            n_file,
            n_data_pos,
            n_data_pos_undo,
        })
    }
}

impl fmt::Debug for BlockIndexRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockIndexRecord")
            .field("block_hash", &self.block_hash)
            .field("version", &self.version)
            .field("height", &self.height)
            .field("status", &self.status)
            .field("n_tx", &self.n_tx)
            .field("n_file", &self.n_file)
            .field("n_data_pos", &self.n_data_pos)
            .field("n_data_pos_undo", &self.n_data_pos_undo)
            .finish()
    }
}

pub fn get_block_index(path: &Path) -> Result<Vec<BlockIndexRecord>, Error> {
    println!("Reading index from {} ...", path.display());

    // let mut block_index = Vec::with_capacity(2000000);
    let mut block_counter = 0;
    let mut header_counter = 0;
    let mut block_index = Vec::new();
    println!("about to open db, path = {:?}", path);
    let mut db = DB::open(path, Options::default())?;
    println!("opened db");
    let mut iter = db.new_iter()?;
    let (mut k, mut v) = (vec![], vec![]);
    println!("declared variables, about to get the block index");
    while iter.advance() {
        // println!("counter = {}", counter);
        iter.current(&mut k, &mut v);
        if is_block_index_record(&k) {
            let record = BlockIndexRecord::from(&k[1..], &v)?;
            if record.status & (BLOCK_VALID_CHAIN | BLOCK_HAVE_DATA | BLOCK_VALID_CHAIN) > 0 {
                block_index.push(record);
                block_counter += 1;
            }
            header_counter += 1;
        }
    }
    // println!("got block index, sorting...");
    block_index.sort_by_key(|b| b.height);
    println!("Got longest chain with {} blocks ... and {} header_count and block_count {}", block_index.len(), header_counter, block_counter);
    Ok(block_index)
}

#[inline]
fn is_block_index_record(data: &[u8]) -> bool {
    *data.get(0).unwrap() == b'b'
}

/// TODO: this is a wonky 1:1 translation from https://github.com/bitcoin/bitcoin
/// It is NOT the same as CompactSize.
pub fn read_varint(reader: &mut Cursor<&[u8]>) -> Result<usize, Error> {
    let mut n = 0;
    loop {
        let ch_data = reader.read_u8()?;
        if n > usize::max_value() >> 7 {
            panic!("size too large");
        }
        n = (n << 7) | (ch_data & 0x7F) as usize;
        if ch_data & 0x80 > 0 {
            if n == usize::max_value() {
                panic!("size too large");
            }
            n += 1;
        } else {
            break;
        }
    }
    Ok(n)
}
