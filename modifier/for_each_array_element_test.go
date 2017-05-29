package modifier_test

import (
	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/modifier"
	"github.com/draganm/immersadb/store"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ForEachArrayElement", func() {
	var s *store.MemoryStore
	var m *modifier.Modifier
	// var err error
	BeforeEach(func() {
		s = store.NewMemoryStore([]byte{
			// Hash Root Chunk
			0, 0, 0, 4,
			//
			0, 20,
			0, 0,
			0, 0, 0, 4,
		})
		_, err := s.Append(chunk.NewCommitChunk(0))
		Expect(err).ToNot(HaveOccurred())

		m = modifier.New(s, 8192, chunk.LastCommitRootHashAddress(s))
	})

	var indexes []uint64

	JustBeforeEach(func() {
		mr, err := m.EntityReaderFor(modifier.DBPath{"test"})
		Expect(err).ToNot(HaveOccurred())

		indexes = nil
		mr.ForEachArrayElement(func(index uint64, _ modifier.EntityReader) error {
			indexes = append(indexes, index)
			return nil
		})
	})

	Context("When I create an array", func() {
		BeforeEach(func() {
			Expect(m.CreateArray(modifier.DBPath{"test"})).To(Succeed())
		})

		It("Should not call iterator function", func() {
			Expect(indexes).To(BeNil())
		})
		Context("When I add one element to the array", func() {
			BeforeEach(func() {
				Expect(m.CreateHash(modifier.DBPath{"test", 0})).To(Succeed())
			})

			It("Should call iterator function once", func() {
				Expect(indexes).To(Equal([]uint64{0}))
			})

			Context("When I add three more elements", func() {
				BeforeEach(func() {
					for i := 0; i < 3; i++ {
						Expect(m.CreateHash(modifier.DBPath{"test", 0})).To(Succeed())
					}
				})
				It("Should call iterator function four times", func() {
					Expect(indexes).To(Equal([]uint64{0, 1, 2, 3}))
				})

				Context("When I add fifth element", func() {
					BeforeEach(func() {
						Expect(m.CreateHash(modifier.DBPath{"test", 0})).To(Succeed())
					})
					It("Should call iterator function five times", func() {
						Expect(indexes).To(Equal([]uint64{0, 1, 2, 3, 4}))
					})

					Context("When I add 1200 more elements", func() {
						BeforeEach(func() {
							for i := 0; i < 1200; i++ {
								Expect(m.CreateHash(modifier.DBPath{"test", 0})).To(Succeed())
							}

						})
						It("Should call iterator function 1205 times", func() {
							Expect(len(indexes)).To(Equal(1205))
						})
					})
				})

			})

		})
	})

})
