package modifier_test

import (
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
			0, 10,
			0, 0,
			0, 0, 0, 4,
		})
		m = modifier.New(s, 8192, s.LastChunkAddress())
	})

	Context("When I create an array", func() {
		BeforeEach(func() {
			Expect(m.CreateArray(modifier.DBPath{"test"})).To(Succeed())
		})
		It("Should create empty array leaf", func() {
			Expect(s.Data()).To(Equal([]byte{
				// old root
				0, 0, 0, 4,
				0, 10,
				0, 0,
				0, 0, 0, 4,

				// empty array leaf
				0, 0, 0, 4,
				0, 40,
				0, 0,
				0, 0, 0, 4,

				// new root
				0, 0, 0, 18,
				0, 10,

				// refs
				0, 1,
				0, 0, 0, 0, 0, 0, 0, 12,

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
						0, 10,
						0, 0,
						0, 0, 0, 4,

						// One element Array Leaf
						0, 0, 0, 12,
						0, 40,
						0, 1,
						0, 0, 0, 0, 0, 0, 8, 70,
						0, 0, 0, 12,

						// Empty Array Leaf
						0, 0, 0, 4,
						0, 40,
						0, 0,
						0, 0, 0, 4,

						// Array Node pointing to a two leafs
						0, 0, 0, 70,
						0, 41,
						0, 4,
						0, 0, 0, 0, 0, 0, 8, 102,
						0, 0, 0, 0, 0, 0, 8, 102,
						0, 0, 0, 0, 0, 0, 8, 82,
						0, 0, 0, 0, 0, 0, 7, 222,
						0, 2,
						0, 0, 0, 0, 0, 0, 0, 0,
						0, 0, 0, 0, 0, 0, 0, 0,
						0, 0, 0, 0, 0, 0, 0, 1,
						0, 0, 0, 0, 0, 0, 0, 16,
						0, 0, 0, 70,

						// New Root
						0, 0, 0, 18,
						0, 10,
						0, 1,
						0, 0, 0, 0, 0, 0, 8, 114,
						0, 4, 116, 101, 115, 116,
						0, 0, 0, 18,
					}))
				})

			})

		})

		Context("When I prepend an empty hash to the array", func() {
			BeforeEach(func() {
				Expect(m.CreateHash(modifier.DBPath{"test", 0})).To(Succeed())
			})

			It("Should create array leaf with one element", func() {
				Expect(s.Data()[50:]).To(Equal([]byte{
					// empty hash
					0, 0, 0, 4,
					0, 10,
					0, 0,
					0, 0, 0, 4,

					// array leaf
					0, 0, 0, 12,
					0, 40,
					// pointer to the empty hash
					0, 1,
					0, 0, 0, 0, 0, 0, 0, 50,
					0, 0, 0, 12,

					// new root
					0, 0, 0, 18,
					0, 10,
					0, 1,
					0, 0, 0, 0, 0, 0, 0, 62,
					0, 4, 116, 101, 115, 116,
					0, 0, 0, 18,
				}))

			})
			Context("When I prepend a second empty hash to the array", func() {
				BeforeEach(func() {
					Expect(m.CreateHash(modifier.DBPath{"test", 0})).To(Succeed())
				})

				It("Should create an array leaf with two elements", func() {
					Expect(s.Data()[108:]).To(Equal([]byte{
						// Empty Hash
						0, 0, 0, 4,
						0, 10,
						0, 0,
						0, 0, 0, 4,

						// Array leaf with two values
						0, 0, 0, 20,
						0, 40,
						0, 2,
						0, 0, 0, 0, 0, 0, 0, 108,
						0, 0, 0, 0, 0, 0, 0, 50,
						0, 0, 0, 20,

						// new root
						0, 0, 0, 18,
						0, 10,
						0, 1, 0, 0, 0, 0, 0, 0, 0, 120,
						0, 4, 116, 101, 115, 116,
						0, 0, 0, 18,
					}))
				})

				Context("When I prepend three more empty hashes to the array", func() {
					BeforeEach(func() {
						for i := 0; i < 3; i++ {
							Expect(m.CreateHash(modifier.DBPath{"test", 0})).To(Succeed())
						}
					})
					It("Should a level 1 array node and two elements", func() {
						Expect(s.Data()[174:]).To(Equal([]byte{
							// Empty Hash
							0, 0, 0, 4,
							0, 10,
							0, 0,
							0, 0, 0, 4,

							// Array leaf with 3 elements
							0, 0, 0, 28,
							0, 40,
							0, 3,
							0, 0, 0, 0, 0, 0, 0, 174,
							0, 0, 0, 0, 0, 0, 0, 108,
							0, 0, 0, 0, 0, 0, 0, 50,
							0, 0, 0, 28,

							// New root
							0, 0, 0, 18,
							0, 10,
							0, 1,
							0, 0, 0, 0, 0, 0, 0, 186,
							0, 4, 116, 101, 115, 116,
							0, 0, 0, 18,

							// Empty Hash
							0, 0, 0, 4,
							0, 10,
							0, 0,
							0, 0, 0, 4,

							// Array Leaf with 4 elements
							0, 0, 0, 36,
							0, 40,
							0, 4,
							0, 0, 0, 0, 0, 0, 0, 248,
							0, 0, 0, 0, 0, 0, 0, 174,
							0, 0, 0, 0, 0, 0, 0, 108,
							0, 0, 0, 0, 0, 0, 0, 50,
							0, 0, 0, 36,

							// New Root
							0, 0, 0, 18,
							0, 10,
							0, 1,
							0, 0, 0, 0, 0, 0, 1, 4,
							0, 4, 116, 101, 115, 116,
							0, 0, 0, 18,

							//  Empty Hash
							0, 0, 0, 4,
							0, 10,
							0, 0,
							0, 0, 0, 4,

							// array leaf with one element
							0, 0, 0, 12,
							0, 40,
							0, 1,
							0, 0, 0, 0, 0, 0, 1, 74,
							0, 0, 0, 12,

							// another empty array leaf
							0, 0, 0, 4,
							0, 40,
							0, 0,
							0, 0, 0, 4,

							//
							0, 0, 0, 70,
							0, 41,
							0, 4,
							0, 0, 0, 0, 0, 0, 1, 106,
							0, 0, 0, 0, 0, 0, 1, 106,
							0, 0, 0, 0, 0, 0, 1, 86,
							0, 0, 0, 0, 0, 0, 1, 4,
							0, 1,
							0, 0, 0, 0, 0, 0, 0, 0,
							0, 0, 0, 0, 0, 0, 0, 0,
							0, 0, 0, 0, 0, 0, 0, 1,
							0, 0, 0, 0, 0, 0, 0, 4,
							0, 0, 0, 70,

							// New Root
							0, 0, 0, 18,
							0, 10,
							0, 1,
							0, 0, 0, 0, 0, 0, 1, 118,
							0, 4, 116, 101, 115, 116,
							0, 0, 0, 18,
						}))
						Expect(len(s.Data())).To(Equal(478))
					})

					Context("When I prepend another empty hash to the array", func() {
						BeforeEach(func() {
							Expect(m.CreateHash(modifier.DBPath{"test", 0})).To(Succeed())
						})
						It("Should a level 1 array node and two elements", func() {
							Expect(s.Data()[478:]).To(Equal([]byte{
								// empty hash
								0, 0, 0, 4,
								0, 10,
								0, 0,
								0, 0, 0, 4,

								// two element array leaf
								0, 0, 0, 20,
								0, 40,
								0, 2,
								0, 0, 0, 0, 0, 0, 1, 222,
								0, 0, 0, 0, 0, 0, 1, 74,
								0, 0, 0, 20,

								// Array node pointing to two leafs
								0, 0, 0, 70,
								0, 41,
								0, 4,
								0, 0, 0, 0, 0, 0, 1, 106,
								0, 0, 0, 0, 0, 0, 1, 106,
								0, 0, 0, 0, 0, 0, 1, 234,
								0, 0, 0, 0, 0, 0, 1, 4,
								0, 1,
								0, 0, 0, 0, 0, 0, 0, 0,
								0, 0, 0, 0, 0, 0, 0, 0,
								0, 0, 0, 0, 0, 0, 0, 2,
								0, 0, 0, 0, 0, 0, 0, 4,
								0, 0, 0, 70,

								// new root
								0, 0, 0, 18,
								0, 10,
								0, 1,
								0, 0, 0, 0, 0, 0, 2, 6,
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
