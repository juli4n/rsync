// Copyright 2016 Julian Gutierrez Oschmann (github.com/juli4n).
// All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

// Package rsync contains an implementation of the rsync algorithm.
package rsync

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
)

const (
	BlockSize = 1024 * 64
	M         = 1 << 16
)

// BlockHash holds both strong and weak hash of a block.
type BlockHash struct {
	index      int
	strongHash []byte
	weakHash   uint32
}

// There are two kind of operations: BLOCK and DATA.
// If a block match is found on the server, a BLOCK operation is sent over the channel along with the block index.
// Modified data between two block matches is sent like a DATA operation.
const (
	BLOCK = iota
	DATA
)

// Operation represents a rsync operation (typically to be sent across the network).
// It can be either a block of raw data or a block index.
type Operation struct {
	// Kind of operation: BLOCK | DATA.
	OpCode int
	// The raw modificated (or misaligned) data. Iff opCode == DATA, nil otherwise.
	Data []byte
	// The index of found block. Iff opCode == BLOCK. nil otherwise.
	BlockIndex int
}

// CalculateBlockHashes returns a list of block hashes for a given byte stream.
func CalculateBlockHashes(content io.Reader) ([]BlockHash, error) {
	b := make([]byte, BlockSize)
	var blockHashes []BlockHash
	i := 0
	for {
		n, err := content.Read(b)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		weak, _, _ := weakHash(b[0:n])
		strong := strongHash(b[0:n])
		blockHashes = append(blockHashes, BlockHash{index: i, strongHash: strong, weakHash: weak})
		i++
	}
	return blockHashes, nil
}

// ApplyOps applies operations from a given channel to the original content
// and returns a reader with the modified content.
func ApplyOps(original io.Reader, ops chan Operation) (io.Reader, error) {
	if s, ok := original.(io.ReadSeeker); ok {
		return applyOpsSeeker(s, ops), nil
	}
	b, err := ioutil.ReadAll(original)
	if err != nil {
		return nil, err
	}
	return applyOpsSeeker(bytes.NewReader(b), ops), nil
}

func applyOpsSeeker(original io.ReadSeeker, ops chan Operation) io.Reader {
	r, w := io.Pipe()
	go func() {
		for op := range ops {
			switch op.OpCode {
			case BLOCK:
				// There is a block match, set the reader offset to the right position
				// and do the copy. If something goes wrong, just close the writer with an error.
				if _, err := original.Seek(int64(op.BlockIndex*BlockSize), io.SeekStart); err != nil {
					w.CloseWithError(err)
				}
				if n, err := io.CopyN(w, original, BlockSize); err != nil {
					w.CloseWithError(err)
				} else if n != BlockSize {
					w.CloseWithError(fmt.Errorf("Cannot read block at offset %d", op.BlockIndex*BlockSize))
				}
			case DATA:
				// There is no block match. Copy the raw data.
				w.Write(op.Data)
			}
		}
		w.Close()
	}()
	return r
}

// CalculateDifferences computes all the operations needed to recreate content.
// All these operations are sent through a channel of RSyncOp.
func CalculateDifferences(content []byte, hashes []BlockHash, opsChannel chan Operation) {
	hashesMap := make(map[uint32][]BlockHash)
	defer close(opsChannel)

	for _, h := range hashes {
		key := h.weakHash
		hashesMap[key] = append(hashesMap[key], h)
	}

	var offset, previousMatch int
	var aweak, bweak, weak uint32
	var dirty, isRolling bool

	for offset < len(content) {
		endingByte := min(offset+BlockSize, len(content)-1)
		block := content[offset:endingByte]
		if !isRolling {
			weak, aweak, bweak = weakHash(block)
			isRolling = true
		} else {
			aweak = (aweak - uint32(content[offset-1]) + uint32(content[endingByte-1])) % M
			bweak = (bweak - (uint32(endingByte-offset) * uint32(content[offset-1])) + aweak) % M
			weak = aweak + (1 << 16 * bweak)
		}
		if l := hashesMap[weak]; l != nil {
			blockFound, blockHash := searchStrongHash(l, strongHash(block))
			if blockFound {
				if dirty {
					opsChannel <- Operation{OpCode: DATA, Data: content[previousMatch:offset]}
					dirty = false
				}
				opsChannel <- Operation{OpCode: BLOCK, BlockIndex: blockHash.index}
				previousMatch = endingByte
				isRolling = false
				offset += BlockSize
				continue
			}
		}
		dirty = true
		offset++
	}

	if dirty {
		opsChannel <- Operation{OpCode: DATA, Data: content[previousMatch:]}
	}
}

// Searches for a given strong hash among all strong hashes in this bucket.
func searchStrongHash(l []BlockHash, hashValue []byte) (bool, *BlockHash) {
	for _, blockHash := range l {
		if bytes.Compare(blockHash.strongHash, hashValue) == 0 {
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
func weakHash(v []byte) (uint32, uint32, uint32) {
	var a, b uint32
	for i := range v {
		a += uint32(v[i])
		b += (uint32(len(v)-1) - uint32(i) + 1) * uint32(v[i])
	}
	return (a % M) + (1 << 16 * (b % M)), a % M, b % M
}

// Returns the smaller of a or b.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
