package gc_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/gc"
	"github.com/draganm/immersadb/modifier"
	"github.com/draganm/immersadb/store"

	"testing"
)

func TestGc(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Gc Suite")
}

var _ = Describe("Copy", func() {

	var source *store.MemoryStore
	var destination *store.MemoryStore
	var err error

	BeforeEach(func() {
		source = store.NewMemoryStore(nil)
		destination = store.NewMemoryStore(nil)
	})

	Context("When there only empty hash in the source storage", func() {
		BeforeEach(func() {
			_, err = source.Append(chunk.Pack(chunk.HashLeafType, nil, nil))
			Expect(err).ToNot(HaveOccurred())
		})

		Context("When I copy to the destionation", func() {
			BeforeEach(func() {
				err = gc.Copy(source, destination)
			})
			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should copy the empty hash", func() {
				Expect(destination.Data()).To(Equal([]byte{0, 0, 0, 4, 0, 10, 0, 0, 0, 0, 0, 4}))
			})
		})

		Context("When I add a new value to the source", func() {
			BeforeEach(func() {
				m := modifier.New(source, 1024, source.LastChunkAddress())
				err = m.CreateHash(modifier.DBPath{"test"})
				Expect(err).ToNot(HaveOccurred())
			})
			Context("When I copy to the destionation", func() {
				BeforeEach(func() {
					err = gc.Copy(source, destination)
				})
				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})
				It("Should not contain the old hash", func() {
					Expect(destination.Data()).To(Equal([]byte{
						0, 0, 0, 4,
						0, 10,
						0, 0,
						0, 0, 0, 4,

						0, 0, 0, 18,
						0, 10,
						0, 1,
						0, 0, 0, 0, 0, 0, 0, 0,
						0, 4, 116, 101, 115, 116,
						0, 0, 0, 18}))
				})
			})

			Context("When I add another level of hash to the source", func() {
				BeforeEach(func() {
					m := modifier.New(source, 1024, source.LastChunkAddress())
					err = m.CreateHash(modifier.DBPath{"test", "test2"})
					Expect(err).ToNot(HaveOccurred())
				})
				Context("When I copy to the destionation", func() {
					BeforeEach(func() {
						err = gc.Copy(source, destination)
					})
					It("Should not return error", func() {
						Expect(err).ToNot(HaveOccurred())
					})
					It("Should not contain the old hash", func() {
						Expect(destination.Data()).To(Equal([]byte{
							0, 0, 0, 4,
							0, 10,
							0, 0,
							0, 0, 0, 4,

							0, 0, 0, 19,
							0, 10,
							0, 1,
							0, 0, 0, 0, 0, 0, 0, 0,
							0, 5, 116, 101, 115, 116, 50,
							0, 0, 0, 19,

							0, 0, 0, 18,
							0, 10,
							0, 1,
							0, 0, 0, 0, 0, 0, 0, 12,
							0, 4, 116, 101, 115, 116,
							0, 0, 0, 18,
						}))
					})
				})

				Context("When I add another hash to the parallel level", func() {
					BeforeEach(func() {
						m := modifier.New(source, 1024, source.LastChunkAddress())
						err = m.CreateHash(modifier.DBPath{"test", "test3"})
						Expect(err).ToNot(HaveOccurred())
					})
					Context("When I copy to the destionation", func() {
						BeforeEach(func() {
							err = gc.Copy(source, destination)
						})
						It("Should not return error", func() {
							Expect(err).ToNot(HaveOccurred())
						})
						It("Should not contain the old hash", func() {
							Expect(destination.Data()).To(Equal([]byte{
								0, 0, 0, 4,
								0, 10,
								0, 0,
								0, 0, 0, 4,

								0, 0, 0, 34,
								0, 10,
								0, 2,
								0, 0, 0, 0, 0, 0, 0, 0,
								0, 0, 0, 0, 0, 0, 0, 0,
								0, 5, 116, 101, 115, 116, 50,
								0, 5, 116, 101, 115, 116, 51,
								0, 0, 0, 34,

								0, 0, 0, 18,
								0, 10,
								0, 1,
								0, 0, 0, 0, 0, 0, 0, 12,
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
