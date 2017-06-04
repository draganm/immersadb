package gc_test

import (
	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/gc"
	"github.com/draganm/immersadb/modifier"
	"github.com/draganm/immersadb/store"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Evacuate", func() {

	var s *store.MemoryStore
	var err error

	BeforeEach(func() {
		s = store.NewMemoryStore(nil)
	})

	Context("When there only empty hash in the store", func() {
		BeforeEach(func() {
			addr, err := s.Append(chunk.Pack(chunk.HashLeafType, nil, nil))
			Expect(err).ToNot(HaveOccurred())
			_, err = s.Append(chunk.NewCommitChunk(addr))
			Expect(err).ToNot(HaveOccurred())
		})

		Context("When I evacuate everything before the last commit", func() {
			BeforeEach(func() {
				err = gc.Evacuate(s, s.NextChunkAddress())
			})
			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should copy all the data", func() {
				Expect(s.Data()).To(Equal([]byte{
					0, 2,
					20,
					0,

					0, 10,
					1,
					1,
					0, 0, 0, 0, 0, 0, 0, 0,

					0, 2,
					20,
					0,

					0, 10,
					1,
					1,
					0, 0, 0, 0, 0, 0, 0, 16,
				}))
			})
		})

		Context("When I evacuate only the last commit", func() {
			BeforeEach(func() {
				err = gc.Evacuate(s, s.NextChunkAddress()-chunk.CommitChunkSize)
			})
			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should evacuate everything", func() {
				Expect(s.Data()).To(Equal([]byte{
					0, 2,
					20,
					0,

					0, 10,
					1,
					1,
					0, 0, 0, 0, 0, 0, 0, 0,

					0, 2,
					20,
					0,

					0, 10,
					1,
					1,
					0, 0, 0, 0, 0, 0, 0, 16,
				}))
			})
		})

		Context("When I add another commit", func() {
			BeforeEach(func() {

				_, refs, _ := chunk.Parts(s.Chunk(s.NextChunkAddress() - chunk.CommitChunkSize))
				m := modifier.New(s, 8192, refs[0])

				Expect(m.CreateArray(modifier.DBPath{"x"})).To(Succeed())
				_, err = s.Append(chunk.NewCommitChunk(m.RootAddress))
				Expect(err).ToNot(HaveOccurred())

			})

			Context("When I evacuate only the last commit", func() {
				var oldLength int
				BeforeEach(func() {
					oldLength = len(s.Data())
					err = gc.Evacuate(s, s.NextChunkAddress()-chunk.CommitChunkSize)
				})

				It("Should evacuate only referenced values", func() {
					Expect(s.Data()[oldLength:]).To(Equal([]byte{
						0, 2,
						30,
						0,

						0, 13,
						20,
						1, 0, 0, 0, 0, 0, 0, 0, 47,
						0, 1, 120,

						0, 10,
						1,
						1,
						0, 0, 0, 0, 0, 0, 0, 51,
					}))
				})
			})

		})

	})

})
