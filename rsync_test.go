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

// Unit tests for core package
package rsync

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

type filePair struct {
	original string
	modified string

	totalOps   uint64
	hashBlocks uint64
	dataBlocks uint64
	dataBytes  uint64
}

func (p *filePair) String() string {
	return fmt.Sprintf("%s %s", p.original, p.modified)
}

func Test_SyncModifiedContent(t *testing.T) {

	files := []filePair{
		{"golang-original.bmp", "golang-modified.bmp",
			48, 46, 2, 196608},
		{"text-original.txt", "text-modified.txt",
			1, 0, 1, 16},
		{"text-original-empty.txt", "text-modified-empty-not.txt",
			2, 0, 2, 175577},
		{"text-original-empty.txt", "text-original-empty.txt",
			0, 0, 0, 0},
		{"text-modified-empty-not.txt", "text-modified-empty-not-rev.txt",
			2, 0, 2, 175577},
		{"text-modified-empty-not-5mb.txt", "text-modified-empty-not-rev-5mb.txt",
			45, 0, 45, 5794041},
		{"text-modified-empty-not-5mb.txt", "text-modified-empty-not-5mb.txt",
			89, 89, 0, 0},
	}

	for _, filePair := range files {
		original, err := os.Open("test-data/" + filePair.original)
		if err != nil {
			panic(err)
		}
		modified, err := os.Open("test-data/" + filePair.modified)
		if err != nil {
			panic(err)
		}
		modifiedBytes, _ := ioutil.ReadFile("test-data/" + filePair.modified)

		rsync := New()
		rsync.MaxDataSize = 2 << 16 // 128 KiB
		hashes, _ := rsync.CalculateBlockHashes(original)

		opsChannel := make(chan Operation)
		opsChannel2 := make(chan Operation)
		go rsync.CalculateDifferences(context.Background(), modified, rsync.BuildHashMap(hashes), opsChannel)

		var totalOps, dataBlocks, hashBlocks, dataBytes uint64
		go func(chan1, chan2 chan Operation) {
			for op := range chan1 {
				totalOps += 1
				if length := uint64(len(op.Data)); length > 1 {
					dataBlocks += 1
					dataBytes += length
				} else {
					hashBlocks += 1
				}

				chan2 <- op
			}
			close(chan2)
		}(opsChannel, opsChannel2)

		result := rsync.ApplyOps(original, opsChannel2)
		resultBytes, err := ioutil.ReadAll(result)
		if err != nil {
			t.Error(err)
		}

		fmt.Printf("%v:\n\ttotalOps %d /hashBlocks %d / dataBlocks %d / dataBytes %d\n\n", filePair, totalOps, hashBlocks, dataBlocks, dataBytes)
		if bytes.Compare(resultBytes, modifiedBytes) != 0 {
			t.Errorf("rsync did not work as expected for %v", filePair.String())
		}

		if filePair.totalOps != totalOps {
			t.Errorf("Expected %d ops, but encountered %d, for: %v", filePair.totalOps, totalOps, filePair)
		}

		if filePair.hashBlocks != hashBlocks {
			t.Errorf("Expected %d hash blocks, but encountered %d, for: %v", filePair.hashBlocks, hashBlocks, filePair)
		}

		if filePair.dataBlocks != dataBlocks {
			t.Errorf("Expected %d data  blocks, but encountered %d, for: %v", filePair.dataBlocks, dataBlocks, filePair)
		}

		if filePair.dataBytes != dataBytes {
			t.Errorf("Expected %d data bytes, but encountered %d, for: %v", filePair.dataBytes, dataBytes, filePair)
		}
	}
}

func Test_WeakHash(t *testing.T) {
	content := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	expectedWeak := uint32(10813485)
	expectedA := uint32(45)
	expectedB := uint32(165)
	weak, a, b := weakHash(content)

	assertHash(t, "weak", content, expectedWeak, weak)
	assertHash(t, "a", content, expectedA, a)
	assertHash(t, "b", content, expectedB, b)
}

func assertHash(t *testing.T, name string, content []byte, expected uint32, found uint32) {
	if found != expected {
		t.Errorf("Incorrent "+name+" hash for %v - Expected %d - Found %d", content, expected, found)
	}
}
