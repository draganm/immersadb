package modifier_test

import (
	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/modifier"
	"github.com/draganm/immersadb/store"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Prepend to array", func() {
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

	Context("When I create an array", func() {
		BeforeEach(func() {
			Expect(m.CreateArray(modifier.DBPath{"test"})).To(Succeed())
		})
		It("Should create empty array leaf", func() {
			Expect(s.Data()).To(Equal([]byte{
				// old root
				0, 0, 0, 4,
				0, 20,
				0, 0,
				0, 0, 0, 4,

				// old commit
				0, 0, 0, 12,
				0, 1,
				0, 1,
				0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 12,

				// empty array leaf
				0, 0, 0, 4,
				0, 30,
				0, 0,
				0, 0, 0, 4,

				// new root
				0, 0, 0, 18,
				0, 20,

				// refs
				0, 1,
				0, 0, 0, 0, 0, 0, 0, 32,

				// names
				0, 4, 116, 101, 115, 116,
				0, 0, 0, 18,
			}))
			// Expect(len(s.Data())).To(Equal(0))
		})

		Context("When Array has 16 empty hashes", func() {
			var lastSize int
			BeforeEach(func() {
				for i := 0; i < 16; i++ {
					Expect(m.CreateHash(modifier.DBPath{"test", 0})).To(Succeed())
				}
				lastSize = len(s.Data())
			})

			Context("When I prepend another empty hash", func() {
				BeforeEach(func() {
					Expect(m.CreateHash(modifier.DBPath{"test", 0})).To(Succeed())
				})
				It("Should create second layer array node", func() {
					Expect(s.Data()[lastSize:]).To(Equal([]byte{

						// Empty Hash
						0, 0, 0, 4,
						0, 20,
						0, 0,
						0, 0, 0, 4,

						// One element Array Leaf
						0, 0, 0, 12,
						0, 30,
						0, 1,
						0, 0, 0, 0, 0, 0, 8, 90,
						0, 0, 0, 12,

						// Empty Array Leaf
						0, 0, 0, 4,
						0, 30,
						0, 0,
						0, 0, 0, 4,

						// Array Node pointing to a two leafs
						0, 0, 0, 70,
						0, 31,
						0, 4,
						0, 0, 0, 0, 0, 0, 8, 122,
						0, 0, 0, 0, 0, 0, 8, 122,
						0, 0, 0, 0, 0, 0, 8, 102,
						0, 0, 0, 0, 0, 0, 7, 242,
						0, 2,
						0, 0, 0, 0, 0, 0, 0, 0,
						0, 0, 0, 0, 0, 0, 0, 0,
						0, 0, 0, 0, 0, 0, 0, 1,
						0, 0, 0, 0, 0, 0, 0, 16,
						0, 0, 0, 70,

						// New Root
						0, 0, 0, 18,
						0, 20,
						0, 1,
						0, 0, 0, 0, 0, 0, 8, 134,
						0, 4, 116, 101, 115, 116,
						0, 0, 0, 18,
					}))
				})

			})

		})

		Context("When I prepend an empty hash to the array", func() {
			var oldLast int
			BeforeEach(func() {
				oldLast = int(s.NextChunkAddress())
				Expect(m.CreateHash(modifier.DBPath{"test", 0})).To(Succeed())
			})

			It("Should create array leaf with one element", func() {
				Expect(s.Data()[oldLast:]).To(Equal([]byte{
					// empty hash
					0, 0, 0, 4,
					0, 20,
					0, 0,
					0, 0, 0, 4,

					// array leaf
					0, 0, 0, 12,
					0, 30,
					// pointer to the empty hash
					0, 1,
					0, 0, 0, 0, 0, 0, 0, 70,
					0, 0, 0, 12,

					// new root
					0, 0, 0, 18,
					0, 20,
					0, 1,
					0, 0, 0, 0, 0, 0, 0, 82,
					0, 4, 116, 101, 115, 116,
					0, 0, 0, 18,
				}))

			})
			Context("When I prepend a second empty hash to the array", func() {
				BeforeEach(func() {
					oldLast = int(s.NextChunkAddress())
					Expect(m.CreateHash(modifier.DBPath{"test", 0})).To(Succeed())
				})

				It("Should create an array leaf with two elements", func() {
					Expect(s.Data()[oldLast:]).To(Equal([]byte{
						// Empty Hash
						0, 0, 0, 4,
						0, 20,
						0, 0,
						0, 0, 0, 4,

						// Array leaf with two values
						0, 0, 0, 20,
						0, 30,
						0, 2,
						0, 0, 0, 0, 0, 0, 0, 128,
						0, 0, 0, 0, 0, 0, 0, 70,
						0, 0, 0, 20,

						// new root
						0, 0, 0, 18,
						0, 20,
						0, 1, 0, 0, 0, 0, 0, 0, 0, 140,
						0, 4, 116, 101, 115, 116,
						0, 0, 0, 18,
					}))
				})

				Context("When I prepend three more empty hashes to the array", func() {
					BeforeEach(func() {
						oldLast = int(s.NextChunkAddress())
						for i := 0; i < 3; i++ {
							Expect(m.CreateHash(modifier.DBPath{"test", 0})).To(Succeed())
						}
					})
					It("Should a level 1 array node and two elements", func() {
						Expect(s.Data()[oldLast:]).To(Equal([]byte{
							// Empty Hash
							0, 0, 0, 4,
							0, 20,
							0, 0,
							0, 0, 0, 4,

							// Array leaf with 3 elements
							0, 0, 0, 28,
							0, 30,
							0, 3,
							0, 0, 0, 0, 0, 0, 0, 194,
							0, 0, 0, 0, 0, 0, 0, 128,
							0, 0, 0, 0, 0, 0, 0, 70,
							0, 0, 0, 28,

							// New root
							0, 0, 0, 18,
							0, 20,
							0, 1,
							0, 0, 0, 0, 0, 0, 0, 206,
							0, 4, 116, 101, 115, 116,
							0, 0, 0, 18,

							// Empty Hash
							0, 0, 0, 4,
							0, 20,
							0, 0,
							0, 0, 0, 4,

							// Array Leaf with 4 elements
							0, 0, 0, 36,
							0, 30,
							0, 4,
							0, 0, 0, 0, 0, 0, 1, 12,
							0, 0, 0, 0, 0, 0, 0, 194,
							0, 0, 0, 0, 0, 0, 0, 128,
							0, 0, 0, 0, 0, 0, 0, 70,
							0, 0, 0, 36,

							// New Root
							0, 0, 0, 18,
							0, 20,
							0, 1,
							0, 0, 0, 0, 0, 0, 1, 24,
							0, 4, 116, 101, 115, 116,
							0, 0, 0, 18,

							//  Empty Hash
							0, 0, 0, 4,
							0, 20,
							0, 0,
							0, 0, 0, 4,

							// array leaf with one element
							0, 0, 0, 12,
							0, 30,
							0, 1,
							0, 0, 0, 0, 0, 0, 1, 94,
							0, 0, 0, 12,

							// another empty array leaf
							0, 0, 0, 4,
							0, 30,
							0, 0,
							0, 0, 0, 4,

							//
							0, 0, 0, 70,
							0, 31,
							0, 4,
							0, 0, 0, 0, 0, 0, 1, 126,
							0, 0, 0, 0, 0, 0, 1, 126,
							0, 0, 0, 0, 0, 0, 1, 106,
							0, 0, 0, 0, 0, 0, 1, 24,
							0, 1,
							0, 0, 0, 0, 0, 0, 0, 0,
							0, 0, 0, 0, 0, 0, 0, 0,
							0, 0, 0, 0, 0, 0, 0, 1,
							0, 0, 0, 0, 0, 0, 0, 4,
							0, 0, 0, 70,

							// New Root
							0, 0, 0, 18,
							0, 20,
							0, 1,
							0, 0, 0, 0, 0, 0, 1, 138,
							0, 4, 116, 101, 115, 116,
							0, 0, 0, 18,
						}))
						Expect(len(s.Data())).To(Equal(498))
					})

					Context("When I prepend another empty hash to the array", func() {
						var lastAddr int
						BeforeEach(func() {
							lastAddr = int(s.NextChunkAddress())
							Expect(m.CreateHash(modifier.DBPath{"test", 0})).To(Succeed())
						})
						It("Should a level 1 array node and two elements", func() {
							Expect(s.Data()[lastAddr:]).To(Equal([]byte{
								0, 0, 0, 4,
								0, 20,
								0, 0,
								0, 0, 0, 4,

								0, 0, 0, 20,
								0, 30,
								0, 2,
								0, 0, 0, 0, 0, 0, 1, 242,
								0, 0, 0, 0, 0, 0, 1, 94,
								0, 0, 0, 20,

								0, 0, 0, 70,
								0, 31,
								0, 4,
								0, 0, 0, 0, 0, 0, 1, 126,
								0, 0, 0, 0, 0, 0, 1, 126,
								0, 0, 0, 0, 0, 0, 1, 254,
								0, 0, 0, 0, 0, 0, 1, 24,
								0, 1,
								0, 0, 0, 0, 0, 0, 0, 0,
								0, 0, 0, 0, 0, 0, 0, 0,
								0, 0, 0, 0, 0, 0, 0, 2,
								0, 0, 0, 0, 0, 0, 0, 4,
								0, 0, 0, 70,

								0, 0, 0, 18,
								0, 20,
								0, 1,
								0, 0, 0, 0, 0, 0, 2, 26,
								0, 4, 116, 101, 115, 116,
								0, 0, 0, 18,
							}))
						})
					})

				})
			})
		})
	})

})
