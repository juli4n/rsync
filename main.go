package main

import "io/ioutil"
import "crypto/md5"
import "container/list"
import "os"
import "log"
import "fmt"

const (
  BlockSize = 1024 * 64
  M = 1<<16
)

type FileHash struct {
  hashes []BlockHash
}

type BlockHash struct {
  index int
  strongHash []byte
  weakHash uint32
}

func getBlockNumber(content []byte) int {
  blockNumber := (len(content) / BlockSize)
  if (len(content) % BlockSize != 0) {
    blockNumber += 1
  }
  return blockNumber
}

func calculateBlockHashes(content []byte) FileHash {

  blockHashes := make([]BlockHash, getBlockNumber(content))

  for i := range(blockHashes) {
    initialByte := i * BlockSize
    endingByte := min((i+1)*BlockSize, len(content))
    block := content[initialByte:endingByte]
    weak,_,_ := weakHash(block)
    blockHashes[i] = BlockHash{index: i, strongHash: strongHash(block), weakHash: weak }
  }

  //log.Printf("File %s \n Hashes: \n %v", os.Args[1], blockHashes)
  return FileHash{ hashes : blockHashes }
}

func applyOps(content []byte, ops chan RSyncOp, fileSize int) []byte {
  result := make([]byte, fileSize)
  offset := 0
  data := 0
  blocks := 0
  for op := range(ops) {
    switch op.opCode {
      case BLOCK:
        copy(result[offset:offset+BlockSize], content[op.blockIndex * BlockSize: op.blockIndex * BlockSize + BlockSize])
        offset += BlockSize
        blocks += 1
      case DATA:
        copy(result[offset:], op.data)
        offset += len(op.data)
        data += len(op.data)
    }
  }
  log.Printf("Total amout of data transfered: %d. Number of blocks found: %d out of %d", data, blocks, (len(content) / BlockSize)+1)
  return result
}

func strongHash(v []byte) []byte {
  h := md5.New()
  h.Write(v)
  return h.Sum(nil)
}

const (
  BLOCK = iota
  DATA
)

type RSyncOp struct {
  opCode int
  data []byte
  blockIndex int
}

func calculateDifferences(content []byte, fileHashes FileHash, channel chan RSyncOp) {

  hashesMap := make(map[uint32]*list.List)
  defer close(channel)

  for h := range(fileHashes.hashes) {
    key := fileHashes.hashes[h].weakHash
    if hashesMap[key] == nil {
     hashesMap[key] = &list.List{}
    }
    hashesMap[key].PushBack(fileHashes.hashes[h])
  }

  offset := 0
  previousMatch := 0
  var aweak uint32 = 0
  var bweak uint32 = 0
  var weak uint32 = 0
  dirtyBytes := false
  first := true
  for offset < len(content) {
    endingByte := min(offset + BlockSize, len(content)-1)
    block := content[offset:endingByte]
    if first {
      weak, aweak, bweak = weakHash(block)
      first = false
    } else {
      aweak = (aweak - uint32(content[offset-1]) + uint32(content[endingByte-1])) % M
      bweak = (bweak - (uint32(endingByte - offset) * uint32(content[offset-1])) + aweak) % M
      weak = aweak + (1<<16 * bweak)
 //     weak,_,_ = weakHash(block)
    }

    newOffset := offset + 1
    if hashesMap[weak] != nil {
      strong := strongHash(block)
      l := hashesMap[weak]
      blockFound, blockHash := searchStrongHash(l, strong)
      if blockFound {
        if (dirtyBytes) {
          channel <- RSyncOp{ opCode : DATA, data: content[previousMatch:offset]}
        }
        channel <- RSyncOp{ opCode : BLOCK, blockIndex: blockHash.index}
        previousMatch = endingByte
        newOffset = offset + BlockSize
        first = true
      } else {
        dirtyBytes = true
      }
    } else {
      dirtyBytes = true
    }

    //log.Printf("Offset %d, weak %d",offset, weak)
    offset = newOffset
  }

  if (dirtyBytes) {
    channel <- RSyncOp{ opCode : DATA, data: content[previousMatch:]}
  }
}

func searchStrongHash(l *list.List, hashValue []byte) (bool,*BlockHash) {
  for e := l.Front(); e != nil; e = e.Next() {
    blockHash := e.Value.(BlockHash)
    if string(blockHash.strongHash) == string(hashValue) {
      return true, &blockHash
    }
  }
  return false, nil
}

func weakHash(v []byte) (uint32,uint32,uint32) {
  var a uint32 = 0
  var b uint32 = 0
  for i := range(v) {
    a += uint32(v[i])
    b += (uint32(len(v)-1)-uint32(i)+1)*uint32(v[i])
  }
  return (a%M) + (1<<16 * (b%M)), a % M, b % M
}

func min (a,b int) int {
  if a < b {
    return a;
  }
  return b;
}

func main() {
  var err error
  var source []byte
  var target []byte
  if source, err = ioutil.ReadFile(os.Args[1]); err != nil {
    log.Fatalf("File %s cannot be found", os.Args[1])
  }

  if target, err = ioutil.ReadFile(os.Args[2]); err != nil {
    log.Fatalf("File %s cannot be found", os.Args[2])
  }

  fh := calculateBlockHashes(target)
  channel := make(chan RSyncOp)
  go calculateDifferences(source, fh, channel)
  //for  _ = range(channel) {
    //log.Printf("Operation: %v", op)
  //}
  result := applyOps(target, channel, len(source))
  fmt.Print(string(result))
}
