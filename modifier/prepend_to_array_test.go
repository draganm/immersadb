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
			0, 2,
			//
			20,
			0,
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
				0, 2,
				20,
				0,

				// old commit
				0, 10,
				1,
				1,
				0, 0, 0, 0, 0, 0, 0, 0,

				// empty array leaf
				0, 2,
				30,
				0,

				// new root
				0, 16,
				20,
				// refs
				1,
				0, 0, 0, 0, 0, 0, 0, 16,

				// names
				0, 4, 116, 101, 115, 116,
			}))

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
						0, 2,
						20,
						0,

						// One element Array Leaf
						0, 10,
						30,
						1,
						0, 0, 0, 0, 0, 0, 6, 82,

						// Empty Array Leaf
						0, 2,
						30,
						0,

						// Array Node pointing to a two leafs
						0, 68,
						31,
						4,
						0, 0, 0, 0, 0, 0, 6, 98,
						0, 0, 0, 0, 0, 0, 6, 98,
						0, 0, 0, 0, 0, 0, 6, 86,
						0, 0, 0, 0, 0, 0, 5, 250,
						0, 2,
						0, 0, 0, 0, 0, 0, 0, 0,
						0, 0, 0, 0, 0, 0, 0, 0,
						0, 0, 0, 0, 0, 0, 0, 1,
						0, 0, 0, 0, 0, 0, 0, 16,

						// New Root
						0, 16,
						20,
						1,
						0, 0, 0, 0, 0, 0, 6, 102,
						0, 4, 116, 101, 115, 116,
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
					0, 2,
					20,
					0,

					// array leaf
					0, 10,
					30,
					// pointer to the empty hash
					1,
					0, 0, 0, 0, 0, 0, 0, 38,

					// new root
					0, 16,
					20,
					1,
					0, 0, 0, 0, 0, 0, 0, 42,
					0, 4, 116, 101, 115, 116,
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
						0, 2,
						20,
						0,

						// Array leaf with two values
						0, 18,
						30,
						2,
						0, 0, 0, 0, 0, 0, 0, 72,
						0, 0, 0, 0, 0, 0, 0, 38,

						// new root
						0, 16,
						20,
						1, 0, 0, 0, 0, 0, 0, 0, 76,
						0, 4, 116, 101, 115, 116,
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
							0, 2,
							20,
							0,

							// Array leaf with 3 elements
							0, 26,
							30,
							3,
							0, 0, 0, 0, 0, 0, 0, 114,
							0, 0, 0, 0, 0, 0, 0, 72,
							0, 0, 0, 0, 0, 0, 0, 38,

							// New root
							0, 16,
							20,
							1,
							0, 0, 0, 0, 0, 0, 0, 118,
							0, 4, 116, 101, 115, 116,

							// Empty Hash
							0, 2,
							20,
							0,

							// Array Leaf with 4 elements
							0, 34,
							30,
							4,
							0, 0, 0, 0, 0, 0, 0, 164,
							0, 0, 0, 0, 0, 0, 0, 114,
							0, 0, 0, 0, 0, 0, 0, 72,
							0, 0, 0, 0, 0, 0, 0, 38,

							// New Root
							0, 16,
							20,
							1,
							0, 0, 0, 0, 0, 0, 0, 168,
							0, 4, 116, 101, 115, 116,

							//  Empty Hash
							0, 2,
							20,
							0,

							// array leaf with one element
							0, 10,
							30,
							1,
							0, 0, 0, 0, 0, 0, 0, 222,

							// another empty array leaf
							0, 2,
							30,
							0,

							//
							0, 68,
							31,
							4,
							0, 0, 0, 0, 0, 0, 0, 238,
							0, 0, 0, 0, 0, 0, 0, 238,
							0, 0, 0, 0, 0, 0, 0, 226,
							0, 0, 0, 0, 0, 0, 0, 168,
							0, 1,
							0, 0, 0, 0, 0, 0, 0, 0,
							0, 0, 0, 0, 0, 0, 0, 0,
							0, 0, 0, 0, 0, 0, 0, 1,
							0, 0, 0, 0, 0, 0, 0, 4,

							// New Root
							0, 16,
							20,
							1,
							0, 0, 0, 0, 0, 0, 0, 242,
							0, 4, 116, 101, 115, 116,
						}))
					})

					Context("When I prepend another empty hash to the array", func() {
						var lastAddr int
						BeforeEach(func() {
							lastAddr = int(s.NextChunkAddress())
							Expect(m.CreateHash(modifier.DBPath{"test", 0})).To(Succeed())
						})
						It("Should a level 1 array node and two elements", func() {
							Expect(s.Data()[lastAddr:]).To(Equal([]byte{
								0, 2,
								20,
								0,

								0, 18,
								30,
								2,
								0, 0, 0, 0, 0, 0, 1, 74,
								0, 0, 0, 0, 0, 0, 0, 222,

								0, 68,
								31,
								4,
								0, 0, 0, 0, 0, 0, 0, 238,
								0, 0, 0, 0, 0, 0, 0, 238,
								0, 0, 0, 0, 0, 0, 1, 78,
								0, 0, 0, 0, 0, 0, 0, 168,
								0, 1,
								0, 0, 0, 0, 0, 0, 0, 0,
								0, 0, 0, 0, 0, 0, 0, 0,
								0, 0, 0, 0, 0, 0, 0, 2,
								0, 0, 0, 0, 0, 0, 0, 4,

								0, 16,
								20,
								1,
								0, 0, 0, 0, 0, 0, 1, 98,
								0, 4, 116, 101, 115, 116,
							}))
						})
					})

				})
			})
		})
	})

})
