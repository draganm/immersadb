package store_test

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/store"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("GCStore", func() {
	var dir string
	BeforeEach(func() {
		var err error
		dir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		if dir != "" {
			Expect(os.RemoveAll(dir)).To(Succeed())
		}
	})

	Describe("NewGCStore()", func() {
		var err error
		var s *store.GCStore

		JustBeforeEach(func() {
			s, err = store.NewGCStore(dir)
		})

		Context("When there is already a segment file in the dir with non-zero start address", func() {
			BeforeEach(func() {
				_, err := store.NewFileStore(fmt.Sprintf("%s/%016x.seg", dir, 123))
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should return non-nil GCStore", func() {
				Expect(s).ToNot(BeNil())
			})

			It("Should return GCStore with FirstChunkAddress of the segment start", func() {
				Expect(s.FirstChunkAddress()).To(Equal(uint64(123)))
			})

			It("Should return GCStore with NextChunkAddress of the segment end", func() {
				Expect(s.NextChunkAddress()).To(Equal(uint64(123)))
			})

			Context("When I append a chunk", func() {
				var chunkAddress uint64
				BeforeEach(func() {
					chunkAddress, err = s.Append(chunk.Pack(chunk.ArrayLeafType, nil, nil))
				})
				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})

				It("Should return chunk address (123)", func() {
					Expect(chunkAddress).To(Equal(uint64(123)))
				})

				Context("When I get NextChunkAddress", func() {
					var nextChunkAddress uint64

					BeforeEach(func() {
						nextChunkAddress = s.NextChunkAddress()
					})

					It("Should return 127", func() {
						Expect(nextChunkAddress).To(Equal(uint64(123 + 4)))
					})
				})

				Context("When I read the chunk", func() {
					var c []byte
					BeforeEach(func() {
						c = s.Chunk(chunkAddress)
					})
					It("Should return the chunk", func() {
						Expect(c).To(Equal([]byte{30, 0}))
					})
				})
			})

		})

		Context("When dir is empty", func() {
			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should return non-nil GCStore", func() {
				Expect(s).ToNot(BeNil())
			})

			It("Should return GCStore with FirstChunkAddress 0", func() {
				Expect(s.FirstChunkAddress()).To(Equal(uint64(0)))
			})

			It("Should return GCStore with NextChunkAddress 0", func() {
				Expect(s.NextChunkAddress()).To(Equal(uint64(0)))
			})

			Context("When I append a chunk", func() {
				var chunkAddress uint64
				BeforeEach(func() {
					chunkAddress, err = s.Append(chunk.Pack(chunk.ArrayLeafType, nil, nil))
				})
				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})

				It("Should return chunk address (0)", func() {
					Expect(chunkAddress).To(Equal(uint64(0)))
				})

				Context("When I get NextChunkAddress", func() {
					var nextChunkAddress uint64

					BeforeEach(func() {
						nextChunkAddress = s.NextChunkAddress()
					})

					It("Should return 4", func() {
						Expect(nextChunkAddress).To(Equal(uint64(4)))
					})
				})

				Context("When I read the chunk", func() {
					var c []byte
					BeforeEach(func() {
						c = s.Chunk(chunkAddress)
					})
					It("Should return the chunk", func() {
						Expect(c).To(Equal([]byte{30, 0}))
					})
				})

			})

		})

	})

})
