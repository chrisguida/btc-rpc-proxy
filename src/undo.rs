use std::{fmt, io::SeekFrom, usize};
use std::error::{self, Error};
use hex::{FromHex, decode, decode_to_slice};
use serde_json::de::Read;
use std::io::{self};
use std::path::{Path, PathBuf};
use tokio::{fs::File, io::{AsyncReadExt, BufReader, ErrorKind}};
use bitcoin::{
    consensus::{Decodable, Encodable, ReadExt, encode::VarInt},
    hashes::{sha256d, Hash},
    blockdata::undo::BlockUndo,
};


const MAGIC_BYTES: [u8; 4] = [249, 190, 180, 217];
const MAGIC_BYTES_INT: u32 = 3652501241;

pub struct BlockUndoCursor {
    base_path: PathBuf,
    current_file_index: usize,
    current_block_index: usize,
    current_file_offset: usize,
    current_file_path: PathBuf,
    current_file_reader: BufReader<File>,
}

impl BlockUndoCursor {
    pub async fn new(base_path: PathBuf, current_file_index: usize, current_file_offset: usize) -> Self {
        let current_file_path = base_path.join(format!("rev{:05}.dat", current_file_index));
        let mut f = File::open(&current_file_path).await.unwrap();
        if current_file_offset > 0 {
            f.seek(SeekFrom::Start(current_file_offset as u64)).await;
        }
        let current_file_reader = BufReader::new(f);
        BlockUndoCursor {
            base_path: base_path,
            current_file_index: current_file_index,
            current_block_index: 0,
            current_file_offset: current_file_offset,
            current_file_path: current_file_path,
            current_file_reader: current_file_reader,
        }

    }

    pub async fn next(&mut self) -> Option<Result<BlockUndo, anyhow::Error>> {
        println!("reading file {}, offset {}", self.current_file_index, self.current_file_offset);
        // read magic bytes
        let mut magic_bytes_u32 = match self.current_file_reader.read_u32_le().await {
            Ok(n) => {
                println!("found ok result {}", n);
                self.current_file_offset += 4;
                n
            },
            Err(error) => match error.kind() {
                ErrorKind::UnexpectedEof => {
                    println!("found unexpected eof");
                    self.next_file().await;
                    self.current_file_offset += 4;
                    self.current_file_reader.read_u32_le().await.unwrap()
                },
                other_error => {
                    panic!("Problem opening the file: {:?}", other_error)
                }
            },
        };
        if magic_bytes_u32 == 0 {
            println!("found magic bytes == 0");
            self.next_file().await;
            magic_bytes_u32 = self.current_file_reader.read_u32_le().await.unwrap();
            self.current_file_offset += 4;
        }
        assert_eq!(magic_bytes_u32, MAGIC_BYTES_INT);
        // println!("magic bytes found!");
        // read BlockUndo length
        let len = self.current_file_reader.read_u32_le().await.unwrap();
        self.current_file_offset += 4;
        println!("block length {} found", len);
        
        // read BlockUndo
        let mut block_buf = Vec::with_capacity(len as usize);
        // tokio::io::copy(&mut self.current_file_reader.take(len as u64), &mut block_buf).await.unwrap();
        let block_len = tokio::io::copy(&mut (&mut self.current_file_reader).take(len as u64), &mut block_buf).await.unwrap();
        self.current_file_offset += block_len as usize;
        // let block_len = self.current_file_reader.read_exact(&mut block_buf).await.unwrap();
        println!("block_buf {:?} length {} found", block_buf, block_len);
        
        let mut block_cursor = std::io::Cursor::new(block_buf);
        
        // parse BlockUndo
        let block_undo = BlockUndo::consensus_decode(&mut block_cursor).unwrap();
        println!("block_undo parsed: {:?}", block_undo);

        // read 32-byte checksum to arrive at next block
        let mut checksum_buf = Vec::with_capacity(32);
        let checksum_len= tokio::io::copy(&mut (&mut self.current_file_reader).take(32), &mut checksum_buf).await.unwrap();
        self.current_file_offset += checksum_len as usize;
        self.current_block_index += 1;
        // let checksum_len = self.current_file_reader.read_exact(&mut checksum_buf).await.unwrap();
        println!("checksum_buf {:?} found, length {}", checksum_buf, checksum_len);
        Some(Ok(block_undo))
    }

    async fn next_file(&mut self) {
        self.current_file_index += 1;
        self.current_file_path = self.base_path.join(format!("rev{:05}.dat", self.current_file_index));
        println!("found eof. switching to file {:?}", self.current_file_path);
        self.current_file_reader = BufReader::new(
            match File::open(&self.current_file_path).await {
                Ok(f) => f,
                Err(e) => panic!("Problem opening the file: {:?}", e),
            }
        );
    }
}

#[tokio::test]
async fn test_undo() {
    let count = 1000000;
    let mut c = BlockUndoCursor::new(PathBuf::from("/home/cguida/.bitcoin/blocks"), 0, 0).await;
    for i in 0..=count {
        let block_undo = c.next().await.unwrap().unwrap();
        // println!("block number {} block_undo {:?}", i, block_undo);
        // println!("block number {}", i);
    }
}

