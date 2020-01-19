package data_test

import (
	"crypto/rand"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/immersadb/data"
	"github.com/draganm/immersadb/store"
	"github.com/stretchr/testify/require"
)

func createTempDir(t *testing.T) (string, func() error) {
	td, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	return td, func() error {
		return os.RemoveAll(td)
	}
}

func newTestStore(t *testing.T) (store.Store, func() error) {
	td, cleanup := createTempDir(t)

	l0, err := store.OpenOrCreateSegmentFile(filepath.Join(td, "l0"), 10*1024*1024)
	require.NoError(t, err)

	st := store.Store{l0}

	return st, func() error {
		err = l0.Close()
		if err != nil {
			return err
		}
		return cleanup()
	}
}

func TestStore(t *testing.T) {
	t.Run("data has same length as max segment size", func(t *testing.T) {
		st, cleanup := newTestStore(t)
		defer cleanup()

		dw := data.NewDataWriter(st, 3, 2)

		_, err := dw.Write([]byte{1, 2, 3})

		require.NoError(t, err)

		k, err := dw.Finish()
		require.NoError(t, err)

		f, err := st.Get(k)
		require.False(t, f.HasChildren(), "segment should not have children")
		d, err := f.Specific().DataLeaf()
		require.NoError(t, err)

		require.Equal(t, []byte{1, 2, 3}, d)

	})

	t.Run("data is one byte longer than max segment size", func(t *testing.T) {
		st, cleanup := newTestStore(t)
		defer cleanup()

		dw := data.NewDataWriter(st, 3, 2)

		_, err := dw.Write([]byte{1, 2, 3, 4})

		require.NoError(t, err)

		k, err := dw.Finish()
		require.NoError(t, err)

		require.NoError(t, err)

		f, err := st.Get(k)

		require.Equal(t, store.Segment_specific_Which_dataNode, f.Specific().Which(), "should be a data node segment")

		count := f.Specific().DataNode()
		require.NoError(t, err)

		require.Equal(t, uint64(4), count, "data node should record total size of 4")

		require.True(t, f.HasChildren(), "segment should have children")

		ch, err := f.Children()
		require.NoError(t, err)

		require.Equal(t, 2, ch.Len(), "should have two children")

		t.Run("first child should have first 3 bytes", func(t *testing.T) {
			ck := ch.At(0)

			cf, err := st.Get(store.Address(ck))
			require.NoError(t, err)

			d, err := cf.Specific().DataLeaf()
			require.NoError(t, err)

			require.Equal(t, []byte{1, 2, 3}, d)

		})

		t.Run("second child should have last byte", func(t *testing.T) {
			ck := ch.At(1)

			cf, err := st.Get(store.Address(ck))
			require.NoError(t, err)

			d, err := cf.Specific().DataLeaf()
			require.NoError(t, err)

			require.Equal(t, []byte{4}, d)

		})

		t.Run("reading data should return original data", func(t *testing.T) {
			r, err := data.NewReader(k, st)
			require.NoError(t, err)

			d, err := ioutil.ReadAll(r)
			require.NoError(t, err)

			require.Equal(t, []byte{1, 2, 3, 4}, d)
		})

	})

	t.Run("data size requires two levels of indirection", func(t *testing.T) {
		st, cleanup := newTestStore(t)
		defer cleanup()

		dw := data.NewDataWriter(st, 1, 2)

		_, err := dw.Write([]byte{1, 2, 3, 4})

		require.NoError(t, err)

		k, err := dw.Finish()
		require.NoError(t, err)

		f, err := st.Get(k)

		require.Equal(t, store.Segment_specific_Which_dataNode, f.Specific().Which(), "should be a data node segment")

		size := f.Specific().DataNode()
		require.NoError(t, err)

		require.Equal(t, uint64(4), size, "data node should record total size of 4")

		t.Run("reading data should return original data", func(t *testing.T) {
			r, err := data.NewReader(k, st)
			require.NoError(t, err)

			d, err := ioutil.ReadAll(r)
			require.NoError(t, err)

			require.Equal(t, []byte{1, 2, 3, 4}, d)
		})

	})

	t.Run("reading and writing empty data", func(t *testing.T) {
		st, cleanup := newTestStore(t)
		defer cleanup()

		dw := data.NewDataWriter(st, 5, 2)

		k, err := dw.Finish()
		require.NoError(t, err)

		t.Run("reading data should return original data", func(t *testing.T) {
			r, err := data.NewReader(k, st)
			require.NoError(t, err)

			d, err := ioutil.ReadAll(r)
			require.NoError(t, err)

			require.Equal(t, 0, len(d))

		})

	})

	t.Run("reading and writing large amount of data", func(t *testing.T) {
		st, cleanup := newTestStore(t)
		defer cleanup()

		dw := data.NewDataWriter(st, 5, 2)

		dataSize := 8193

		randomData := make([]byte, dataSize)

		n, err := rand.Read(randomData)
		require.NoError(t, err)
		require.Equal(t, dataSize, n)

		n, err = dw.Write(randomData)

		require.Equal(t, dataSize, n)

		require.NoError(t, err)

		k, err := dw.Finish()
		require.NoError(t, err)

		t.Run("reading data should return original data", func(t *testing.T) {
			r, err := data.NewReader(k, st)
			require.NoError(t, err)

			d, err := ioutil.ReadAll(r)
			require.NoError(t, err)

			require.Equal(t, dataSize, len(d))

			require.Equal(t, randomData, d)
		})

	})

}
