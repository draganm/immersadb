package ttfmap_test

import (
	"github.com/draganm/immersadb/modifier/ttfmap"
	"github.com/draganm/immersadb/store"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestTtfmap(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Ttfmap Suite")
}

var _ = Describe("2-3-4 Map", func() {
	var s *store.MemoryStore
	var err error
	BeforeEach(func() {
		s = store.NewMemoryStore(nil)
	})
	Describe("CreateEmpty", func() {
		BeforeEach(func() {
			_, err = ttfmap.CreateEmpty(s)
		})

		It("Does not return error", func() {
			Expect(err).ToNot(HaveOccurred())
		})

		It("Creates empty leaf", func() {
			Expect(s.Data()).To(Equal([]byte{
				0, 3,
				40,
				0,
				192,
			}))
		})
	})

	Describe("Insert", func() {
		var oldTreeAddr uint64
		var newTreeAddr uint64
		JustBeforeEach(func() {
			newTreeAddr, err = ttfmap.Insert(s, oldTreeAddr, "test", 123)
		})

		BeforeEach(func() {
			oldTreeAddr, err = ttfmap.CreateEmpty(s)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("When inserting into empty leaf", func() {

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Creates leaf with one key", func() {
				Expect(s.Data()[newTreeAddr:]).To(Equal([]byte{
					0, 16,
					40,
					1,
					0, 0, 0, 0, 0, 0, 0, 123,
					145, 164, 116, 101, 115, 116,
				}))
			})

		})
		Context("When there is a lower value in the leaf", func() {
			BeforeEach(func() {
				oldTreeAddr, err = ttfmap.Insert(s, oldTreeAddr, "atest", 122)
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Creates leaf with value after the existing value", func() {
				Expect(s.Data()[newTreeAddr:]).To(Equal([]byte{
					0, 30,
					40,
					2,
					0, 0, 0, 0, 0, 0, 0, 122,
					0, 0, 0, 0, 0, 0, 0, 123,
					146, 165, 97, 116, 101, 115, 116, 164, 116, 101, 115, 116,
				}))
			})

		})

		Context("When there is a higher value in the leaf", func() {
			BeforeEach(func() {
				oldTreeAddr, err = ttfmap.Insert(s, oldTreeAddr, "ztest", 124)
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Creates leaf with value before the existing value", func() {
				Expect(s.Data()[newTreeAddr:]).To(Equal([]byte{
					0, 30,
					40,
					2, 0, 0, 0, 0, 0, 0, 0, 123,
					0, 0, 0, 0, 0, 0, 0, 124,
					146, 164, 116, 101, 115, 116, 165, 122, 116, 101, 115, 116,
				}))
			})
		})

		Context("When there is both higher and lower value in the leaf", func() {
			BeforeEach(func() {
				oldTreeAddr, err = ttfmap.Insert(s, oldTreeAddr, "ztest", 124)
				Expect(err).ToNot(HaveOccurred())
				oldTreeAddr, err = ttfmap.Insert(s, oldTreeAddr, "atest", 122)
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Creates leaf with value before the existing value", func() {
				Expect(s.Data()[newTreeAddr:]).To(Equal([]byte{
					0, 44,
					40,
					3,
					0, 0, 0, 0, 0, 0, 0, 122,
					0, 0, 0, 0, 0, 0, 0, 123,
					0, 0, 0, 0, 0, 0, 0, 124,
					147, 165, 97, 116, 101, 115, 116, 164, 116, 101, 115, 116, 165, 122, 116, 101, 115, 116,
				}))
			})
		})

		Context("When there are three values already in the leaf", func() {
			BeforeEach(func() {
				oldTreeAddr, err = ttfmap.Insert(s, oldTreeAddr, "ztest", 124)
				Expect(err).ToNot(HaveOccurred())
				oldTreeAddr, err = ttfmap.Insert(s, oldTreeAddr, "atest", 122)
				Expect(err).ToNot(HaveOccurred())
			})

			Context("When I set the value of one of the keys to a new value", func() {
				BeforeEach(func() {
					oldTreeAddr, err = ttfmap.Insert(s, oldTreeAddr, "atest", 120)
					Expect(err).ToNot(HaveOccurred())
				})

				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})

				It("Creates leaf with value before the existing value", func() {
					Expect(s.Data()[newTreeAddr:]).To(Equal([]byte{
						0, 44,
						40,
						3,
						0, 0, 0, 0, 0, 0, 0, 120,
						0, 0, 0, 0, 0, 0, 0, 123,
						0, 0, 0, 0, 0, 0, 0, 124,
						147, 165, 97, 116, 101, 115, 116, 164, 116, 101, 115, 116, 165, 122, 116, 101, 115, 116,
					}))
				})

			})

			Context("When I insert a value with a key lower than any other key in the map", func() {
				BeforeEach(func() {
					oldTreeAddr, err = ttfmap.Insert(s, oldTreeAddr, "0test", 121)
					Expect(err).ToNot(HaveOccurred())
				})

				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})

			})
		})

	})
})
