package gc_test

import (
	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/gc"
	"github.com/draganm/immersadb/modifier"
	"github.com/draganm/immersadb/store"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Size", func() {
	var s *store.MemoryStore
	var size uint64

	BeforeEach(func() {
		s = store.NewMemoryStore(nil)
	})

	JustBeforeEach(func() {
		size = gc.Size(s)
	})

	Context("When storage contains an empty hash", func() {
		BeforeEach(func() {
			addr, err := s.Append(chunk.Pack(chunk.HashLeafType, nil, nil))
			Expect(err).ToNot(HaveOccurred())
			_, err = s.Append(chunk.NewCommitChunk(addr))
			Expect(err).ToNot(HaveOccurred())

		})

		It("Should return size of the hash chunk", func() {
			Expect(size).To(Equal(s.BytesInStore()))
		})

		Context("When I add a value to the hash", func() {
			BeforeEach(func() {
				m := modifier.New(s, 1024, chunk.LastCommitRootHashAddress(s))
				Expect(m.CreateArray(modifier.DBPath{"test"})).To(Succeed())
				_, err := s.Append(chunk.NewCommitChunk(m.RootAddress))
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should return size of the hash chunk and commit chunk", func() {
				Expect(size).To(Equal(uint64(58)))
			})

		})
	})
})
