/*
Copyright 2016 Julian Gutierrez Oschmann (github.com/juli4n).
Copyright 2019 Dolf 'Freeaqingme' Schimmel.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package rsync contains an implementation of the rsync algorithm.
package rsync

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"hash"
	"io"
)

const (
	defaultBlockSize   = 1024 * 64
	m                  = 1 << 16
	defaultMaxDataSize = 2 << 20 // 2 MiB
)

// BlockHash holds both strong and weak hash of a block.
type BlockHash struct {
	Index      uint32
	StrongHash []byte
	WeakHash   uint32
}

// There are two kind of operations: BLOCK and DATA.
// If a block match is found on the server, a BLOCK operation is sent over the channel along with the block index.
// Modified data between two block matches is sent like a DATA operation.
const (
	INVALID = iota
	BLOCK
	DATA
)

// Operation represents a rsync operation (typically to be sent across the network).
// It can be either a block of raw data or a block index.
type Operation struct {
	// Kind of operation: BLOCK | DATA.
	OpCode int
	// The raw modified (or misaligned) data. If opCode == DATA, nil otherwise.
	Data []byte
	// The index of found block. If opCode == BLOCK. 0 otherwise.
	BlockIndex uint32
}

type Rsync struct {
	StrongHashAlgo func() hash.Hash
	BlockSize      int64
	MaxDataSize    int64
}

func New() *Rsync {
	return &Rsync{
		StrongHashAlgo: md5.New,
		BlockSize:      defaultBlockSize,
		MaxDataSize:    defaultMaxDataSize, // Must be larger than block size
	}
}

// CalculateBlockHashes returns a list of block hashes for a given byte stream.
func (rsync *Rsync) CalculateBlockHashes(content io.Reader) ([]BlockHash, error) {
	b := make([]byte, rsync.BlockSize)
	var blockHashes []BlockHash
	i := uint32(1)
	for {
		n, err := content.Read(b)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		weak, _, _ := weakHash(b[0:n])
		strong := rsync.strongHash(b[0:n])
		blockHashes = append(blockHashes, BlockHash{Index: i, StrongHash: strong, WeakHash: weak})
		i++
	}
	return blockHashes, nil
}

// ApplyOps applies operations from a given channel to the original content
// and returns a reader with the modified content.
func (rsync *Rsync) ApplyOps(original io.ReadSeeker, ops chan Operation) io.ReadCloser {
	r, w := io.Pipe()
	go func() {
		for op := range ops {
			switch op.OpCode {
			case BLOCK:
				offset := int64(op.BlockIndex-1) * rsync.BlockSize
				// There is a block match, set the reader offset to the right position
				// and do the copy. If something goes wrong, just close the writer with an error.
				if _, err := original.Seek(offset, io.SeekStart); err != nil {
					w.CloseWithError(err)
				}
				if n, err := io.CopyN(w, original, rsync.BlockSize); err != nil {
					w.CloseWithError(err)
				} else if n != rsync.BlockSize {
					w.CloseWithError(fmt.Errorf("Cannot read block at offset %d", offset))
				}
			case DATA:
				// There is no block match. Copy the raw data.
				w.Write(op.Data)
			case INVALID:
				panic("Invalid OP")
			}
		}
		w.Close()
	}()
	return r
}

func (rsync *Rsync) BuildHashMap(hashes []BlockHash) map[uint32][]BlockHash {
	hashesMap := make(map[uint32][]BlockHash)

	for _, h := range hashes {
		key := h.WeakHash
		hashesMap[key] = append(hashesMap[key], h)
	}

	return hashesMap
}

// CalculateDifferences computes all the operations needed to recreate content.
// All these operations are sent through a channel of RSyncOp.
func (rsync *Rsync) CalculateDifferences(ctx context.Context,
	source io.Reader,
	hashesMap map[uint32][]BlockHash,
	opsChannel chan Operation,
) {

	defer close(opsChannel)
	content, err := newSlidingWindowBuf(source, rsync.MaxDataSize)
	if err != nil {
		// now what?
	}

	var offset, previousMatch, size, endingByte int64
	var aweak, bweak, weak uint32
	var dirty, isRolling bool
	var prevLastByte, lastByte byte
	var block []byte

	dirtyContent := make([]byte, rsync.MaxDataSize)

	for true {
		block, size, _ = content.ReadAt(offset, rsync.BlockSize)
		endingByte = offset + size
		if len(block) == 0 {
			break
		}

		if !isRolling {
			weak, aweak, bweak = weakHash(block)
			isRolling = true
		} else {
			prevLastByte, _ = content.ReadByteAt(offset - 1)
			lastByte, _ = content.ReadByteAt(endingByte - 1)

			aweak = (aweak - uint32(prevLastByte) + uint32(lastByte)) % m
			bweak = (bweak - (uint32(endingByte-offset) * uint32(prevLastByte)) + aweak) % m
			weak = aweak + (1 << 16 * bweak)
		}

		if l := hashesMap[weak]; l != nil {
			blockFound, blockHash := rsync.searchStrongHash(l, rsync.strongHash(block))
			if blockFound {
				if dirty {
					dirtyContent, _, _ = content.ReadAt(previousMatch, offset-previousMatch)

					select {
					case opsChannel <- Operation{OpCode: DATA, Data: dirtyContent}:
					case <-ctx.Done():
						return
					}
					dirty = false
				}
				select {
				case opsChannel <- Operation{OpCode: BLOCK, BlockIndex: blockHash.Index}:
				case <-ctx.Done():
					return
				}

				previousMatch = endingByte
				isRolling = false
				offset += rsync.BlockSize
				continue
			}
		}

		if (offset - previousMatch) >= rsync.MaxDataSize {
			dirtyContent, _, _ = content.ReadAt(previousMatch, rsync.MaxDataSize)

			select {
			case opsChannel <- Operation{OpCode: DATA, Data: dirtyContent}:
			case <-ctx.Done():
				return
			}
			previousMatch = offset
			isRolling = false
		}

		dirty = true
		offset++
	}

	if dirty {
		dirtyContent, _, _ = content.ReadAt(previousMatch, offset-previousMatch)

		select {
		case opsChannel <- Operation{OpCode: DATA, Data: dirtyContent}:
		case <-ctx.Done():
			return
		}
	}

}

// Searches for a given strong hash among all strong hashes in this bucket.
func (rsync *Rsync) searchStrongHash(l []BlockHash, hashValue []byte) (bool, *BlockHash) {
	for _, blockHash := range l {
		if bytes.Compare(blockHash.StrongHash, hashValue) == 0 {
			return true, &blockHash
		}
	}
	return false, nil
}

// Returns a strong hash for a given block of data
func (rsync *Rsync) strongHash(v []byte) []byte {
	h := rsync.StrongHashAlgo()
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
	return (a % m) + (1 << 16 * (b % m)), a % m, b % m
}
