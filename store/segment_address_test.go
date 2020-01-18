package store_test

import (
	"testing"

	"github.com/draganm/immersadb/store"
	"github.com/stretchr/testify/require"
)

func TestSegmentAddress(t *testing.T) {
	t.Run("creating address", func(t *testing.T) {
		t.Run("when I create an address with segment 1", func(t *testing.T) {
			a := store.NewAddress(1, 123)
			t.Run("then the position should match", func(t *testing.T) {
				require.Equal(t, uint64(123), a.Position())
			})
			t.Run("then the segment should match", func(t *testing.T) {
				require.Equal(t, 1, a.Segment())
			})
		})
		t.Run("when I create an address with segment 2", func(t *testing.T) {
			a := store.NewAddress(2, 123)
			t.Run("then the position should match", func(t *testing.T) {
				require.Equal(t, uint64(123), a.Position())
			})
			t.Run("then the segment should match", func(t *testing.T) {
				require.Equal(t, 2, a.Segment())
			})
		})
		t.Run("when I create an address with segment 3", func(t *testing.T) {
			a := store.NewAddress(3, 123)
			t.Run("then the position should match", func(t *testing.T) {
				require.Equal(t, uint64(123), a.Position())
			})
			t.Run("then the segment should match", func(t *testing.T) {
				require.Equal(t, 3, a.Segment())
			})
		})

	})
}
