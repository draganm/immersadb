package ttfmap_test

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/draganm/immersadb/modifier/ttfmap"
	"github.com/draganm/immersadb/store"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tree", func() {
	var s *store.MemoryStore
	var rootAddress uint64
	var err error
	BeforeEach(func() {
		s = store.NewMemoryStore(nil)
	})

	BeforeEach(func() {
		rootAddress, err = ttfmap.CreateEmpty(s)
		Expect(err).ToNot(HaveOccurred())
	})

	for i := 10; i < 200; i += 10 {

		Context(fmt.Sprintf("When there are %d elements is the tree", i), func() {

			var expectedKeys []string

			BeforeEach(func() {
				expectedKeys = nil
				perm := rand.Perm(i)
				for _, j := range perm {
					key := fmt.Sprintf("x-%04d", j)
					rootAddress, err = ttfmap.Insert(s, rootAddress, key, uint64(100000+j))
					Expect(err).ToNot(HaveOccurred())
					expectedKeys = append(expectedKeys, key)

				}
				sort.Strings(expectedKeys)
			})

			Context("When I Iterate over all elements", func() {

				var iteratedKeys []string

				BeforeEach(func() {
					// graph.DumpGraph(s, rootAddress)
					iteratedKeys = nil
					err = ttfmap.ForEach(s, rootAddress, func(key string, value uint64) error {
						iteratedKeys = append(iteratedKeys, key)
						return nil
					})
				})

				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})

				It("Should iterate over all keys", func() {
					Expect(iteratedKeys).To(Equal(expectedKeys))
				})
			})

			Context("When I add a new element to the tree", func() {
				BeforeEach(func() {
					rootAddress, err = ttfmap.Insert(s, rootAddress, "aNewElement", 999)
				})

				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})

				Context("When I lookup the new element", func() {
					var foundAddr uint64
					BeforeEach(func() {
						foundAddr, err = ttfmap.Lookup(s, rootAddress, "aNewElement")
					})
					It("Should not return error", func() {
						Expect(err).ToNot(HaveOccurred())
					})
					It("Should finds it's address", func() {
						Expect(foundAddr).To(Equal(uint64(999)))
					})
				})

			})
		})

	}

})
