package array_test

import (
	"github.com/draganm/immersadb/modifier/array"
	"github.com/draganm/immersadb/store"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestArray(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Array Suite")
}

var _ = Describe("Array", func() {
	var s *store.MemoryStore
	var rootAddress uint64
	var err error
	BeforeEach(func() {
		s = store.NewMemoryStore(nil)
	})

	Describe("CreateEmpty()", func() {
		BeforeEach(func() {
			rootAddress, err = array.CreateEmpty(s)
		})
		It("Should not return error", func() {
			Expect(err).ToNot(HaveOccurred())
		})
		It("Should create empty array chunk", func() {
			Expect(s.Data()).To(Equal([]byte{
				0, 4,
				30, 0,
				0, 0,
			}))
		})
	})

	Describe("Prepend()", func() {
		Context("When prepending to an empty array", func() {
			BeforeEach(func() {
				rootAddress, err = array.CreateEmpty(s)
				Expect(err).ToNot(HaveOccurred())
			})
			Context("When I prepend one element", func() {
				BeforeEach(func() {
					rootAddress, err = array.Prepend(s, rootAddress, 100000)
				})
				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})
				It("Should Create a level 0 array with one element", func() {
					Expect(s.Data()[rootAddress:]).To(Equal([]byte{
						0, 12,
						30,
						1,
						0, 0, 0, 0, 0, 1, 134, 160,
						0, 1,
					}))
				})
			})

			Context("When I prepend four elements", func() {
				BeforeEach(func() {
					rootAddress, err = array.Prepend(s, rootAddress, 100000)
					rootAddress, err = array.Prepend(s, rootAddress, 100001)
					rootAddress, err = array.Prepend(s, rootAddress, 100002)
					rootAddress, err = array.Prepend(s, rootAddress, 100003)
				})
				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})
				It("Should Create a level 0 array with four elements", func() {
					Expect(s.Data()[rootAddress:]).To(Equal([]byte{
						0, 36,
						30,
						4,
						0, 0, 0, 0, 0, 1, 134, 163,
						0, 0, 0, 0, 0, 1, 134, 162,
						0, 0, 0, 0, 0, 1, 134, 161,
						0, 0, 0, 0, 0, 1, 134, 160,
						0, 4,
					}))
				})
			})

		})
	})
})
