// Copyright 2012 Julian Gutierrez Oschmann (github.com/julian-gutierrez-o).
// All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

// A Golang implementation of the rsync algorithm
// This package contains the algorithm for both client
// and server side.
package core

import "crypto/md5"
import "container/list"

const (
  BlockSize = 1024 * 64
  M = 1<<16
)

type BlockHash struct {
  index int
  strongHash []byte
  weakHash uint32
}

// There are two kind of operations at the client side. BLOCK and DATA.
// If a block is found on the server, a BLOCK operation is sent over the network along with the block index.
// Data between two found blocks, is sent like a DATA operation. 
const (
  BLOCK = iota
  DATA
)

// An operation to be sent across the network. It can be either a block of raw data or a block index.
type RSyncOp struct {
  // Kind of operation: BLOCK | DATA.
  opCode int
  // The raw modificated (or misaligned) data. Iff opCode == DATA
  data []byte
  // The index of found block. Iff opCode == BLOCK. 
  blockIndex int
}

// Returns weak and strong hashes for a given slice.
func CalculateBlockHashes(content []byte) []BlockHash {
  blockHashes := make([]BlockHash, getBlockNumber(content))
  for i := range(blockHashes) {
    initialByte := i * BlockSize
    endingByte := min((i+1)*BlockSize, len(content))
    block := content[initialByte:endingByte]
    weak,_,_ := weakHash(block)
    blockHashes[i] = BlockHash{index: i, strongHash: strongHash(block), weakHash: weak }
  }
  return blockHashes
}

// Returns the number of blocks for a given slice of content.
func getBlockNumber(content []byte) int {
  blockNumber := (len(content) / BlockSize)
  if (len(content) % BlockSize != 0) {
    blockNumber += 1
  }
  return blockNumber
}

// Applies operations from the channel to the original content.
// Returns the modified content.
func ApplyOps(content []byte, ops chan RSyncOp, fileSize int) []byte {
  var offset int
  result := make([]byte, fileSize)
  for op := range(ops) {
    switch op.opCode {
      case BLOCK:
        copy(result[offset:offset+BlockSize], content[op.blockIndex * BlockSize: op.blockIndex * BlockSize + BlockSize])
        offset += BlockSize
      case DATA:
        copy(result[offset:], op.data)
        offset += len(op.data)
    }
  }
  return result
}

// Computes all the operations needed to recreate content.
// All the operations are sent through a channel of RSyncOp.
func CalculateDifferences(content []byte, hashes []BlockHash, opsChannel chan RSyncOp) {

  hashesMap := make(map[uint32]*list.List)
  defer close(opsChannel)

  for _, h := range(hashes) {
    key := h.weakHash
    if hashesMap[key] == nil {
     hashesMap[key] = &list.List{}
    }
    hashesMap[key].PushBack(h)
  }

  var offset, previousMatch int
  var aweak, bweak, weak uint32
  var dirty, isRolling bool

  for offset < len(content) {
    endingByte := min(offset + BlockSize, len(content)-1)
    block := content[offset:endingByte]
    if !isRolling {
      weak, aweak, bweak = weakHash(block)
      isRolling = true
    } else {
      aweak = (aweak - uint32(content[offset-1]) + uint32(content[endingByte-1])) % M
      bweak = (bweak - (uint32(endingByte - offset) * uint32(content[offset-1])) + aweak) % M
      weak = aweak + (1<<16 * bweak)
    }
    if hashesMap[weak] != nil {
      //strong := strongHash(block)
      l := hashesMap[weak]
      blockFound, blockHash := searchStrongHash(l, strongHash(block))
      if blockFound {
        if (dirty) {
          opsChannel <- RSyncOp{ opCode : DATA, data: content[previousMatch:offset]}
        }
        opsChannel <- RSyncOp{ opCode : BLOCK, blockIndex: blockHash.index}
        previousMatch = endingByte
        isRolling = false
        offset += BlockSize
        continue
      }
    }
    dirty = true
    offset++
  }

  if (dirty) {
    opsChannel <- RSyncOp{ opCode : DATA, data: content[previousMatch:]}
  }
}

// Searchs a given strong hash among all strong hashes in this bucket.
func searchStrongHash(l *list.List, hashValue []byte) (bool,*BlockHash) {
  for e := l.Front(); e != nil; e = e.Next() {
    blockHash := e.Value.(BlockHash)
    if string(blockHash.strongHash) == string(hashValue) {
      return true, &blockHash
    }
  }
  return false, nil
}

// Returns a strong hash for a given block of data
func strongHash(v []byte) []byte {
  h := md5.New()
  h.Write(v)
  return h.Sum(nil)
}

// Returns a weak hash for a given block of data.
func weakHash(v []byte) (uint32,uint32,uint32) {
  var a, b uint32;
  for i := range(v) {
    a += uint32(v[i])
    b += (uint32(len(v)-1)-uint32(i)+1)*uint32(v[i])
  }
  return (a%M) + (1<<16 * (b%M)), a % M, b % M
}

// Returns the smaller of a or b. 
func min (a, b int) int {
  if a < b {
    return a;
  }
  return b;
}
