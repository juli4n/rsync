// Copyright 2012 Julian Gutierrez Oschmann (github.com/julian-gutierrez-o).
// All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

// Unit tests for core package
package rsync

import "testing"
import "io/ioutil"

type filePair struct {
	original string
	modified string
}

func Test_SyncModifiedContent(t *testing.T) {

	files := []filePair{filePair{"golang-original.bmp", "golang-modified.bmp"}, filePair{"text-original.txt", "text-modified.txt"}}

	for _, filePair := range files {
		original, _ := ioutil.ReadFile("test-data/" + filePair.original)
		modified, _ := ioutil.ReadFile("test-data/" + filePair.modified)

		hashes := CalculateBlockHashes(original)
		opsChannel := make(chan RSyncOp)
		go CalculateDifferences(modified, hashes, opsChannel)

		result := ApplyOps(original, opsChannel, len(modified))

		if string(result) != string(modified) {
			t.Errorf("rsync did not work as expected for %v", filePair)
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
