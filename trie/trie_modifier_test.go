package trie_test

import (
	"errors"
	"testing"

	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/trie"
	"github.com/stretchr/testify/require"
)

func TestTrieModifier(t *testing.T) {
	tm := trie.TrieModifier{
		fragment.Modifier{
			Fragment: store.Segment{},
		},
	}

	tm.SetError(errors.New("my error"))

	require.Error(t, tm.Error())
}
