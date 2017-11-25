package store_test

import (
	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/dbpath"
	"github.com/draganm/immersadb/modifier"
	"github.com/draganm/immersadb/modifier/ttfmap"
	"github.com/draganm/immersadb/store"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

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
			addr, err := ttfmap.CreateEmpty(source)
			Expect(err).ToNot(HaveOccurred())
			_, err = source.Append(chunk.NewCommitChunk(addr))
			Expect(err).ToNot(HaveOccurred())
		})

		Context("When I copy to the destionation", func() {
			BeforeEach(func() {
				err = store.Copy(source, destination)
			})
			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
			XIt("Should copy the empty hash", func() {
				Expect(destination.Data()).To(Equal([]byte{
					0, 2,
					20,
					0,

					0, 10,
					1,
					1,
					0, 0, 0, 0, 0, 0, 0, 0,
				}))
			})
		})

		Context("When I add a new value to the source", func() {
			BeforeEach(func() {
				m := modifier.New(source, 1024, store.LastCommitRootHashAddress(source))
				err = m.CreateMap(dbpath.Path{"test"})
				Expect(err).ToNot(HaveOccurred())
				_, err = source.Append(chunk.NewCommitChunk(m.RootAddress))
				Expect(err).ToNot(HaveOccurred())
			})
			Context("When I copy to the destionation", func() {
				BeforeEach(func() {
					err = store.Copy(source, destination)
				})
				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})
				XIt("Should not contain the old hash", func() {
					Expect(destination.Data()).To(Equal([]byte{
						0, 2,
						20,
						0,

						0, 16,
						20,
						1,
						0, 0, 0, 0, 0, 0, 0, 0,
						0, 4, 116, 101, 115, 116,

						0, 10,
						1,
						1,
						0, 0, 0, 0, 0, 0, 0, 4,
					}))
				})
			})

			Context("When I add another level of hash to the source", func() {
				BeforeEach(func() {
					m := modifier.New(source, 1024, store.LastCommitRootHashAddress(source))
					err = m.CreateMap(dbpath.Path{"test", "test2"})
					Expect(err).ToNot(HaveOccurred())
					_, err = source.Append(chunk.NewCommitChunk(m.RootAddress))
					Expect(err).ToNot(HaveOccurred())
				})
				Context("When I copy to the destionation", func() {
					BeforeEach(func() {
						err = store.Copy(source, destination)
					})
					It("Should not return error", func() {
						Expect(err).ToNot(HaveOccurred())
					})
					XIt("Should not contain the old hash", func() {
						Expect(destination.Data()).To(Equal([]byte{
							0, 2,
							20,
							0,

							0, 17,
							20,
							1,
							0, 0, 0, 0, 0, 0, 0, 0,
							0, 5, 116, 101, 115, 116, 50,

							0, 16,
							20,
							1,
							0, 0, 0, 0, 0, 0, 0, 4,
							0, 4, 116, 101, 115, 116,

							0, 10,
							1,
							1,
							0, 0, 0, 0, 0, 0, 0, 23,
						}))
					})
				})

				Context("When I add another hash to the parallel level", func() {
					BeforeEach(func() {
						m := modifier.New(source, 1024, store.LastCommitRootHashAddress(source))
						err = m.CreateMap(dbpath.Path{"test", "test3"})
						Expect(err).ToNot(HaveOccurred())
						_, err = source.Append(chunk.NewCommitChunk(m.RootAddress))
						Expect(err).ToNot(HaveOccurred())

					})
					Context("When I copy to the destionation", func() {
						BeforeEach(func() {
							err = store.Copy(source, destination)
						})
						It("Should not return error", func() {
							Expect(err).ToNot(HaveOccurred())
						})
						XIt("Should not contain the old hash", func() {
							Expect(destination.Data()).To(Equal([]byte{
								0, 2,
								20,
								0,

								0, 32,
								20,
								2,
								0, 0, 0, 0, 0, 0, 0, 0,
								0, 0, 0, 0, 0, 0, 0, 0,
								0, 5, 116, 101, 115, 116, 50, 0, 5, 116, 101, 115, 116, 51,

								0, 16,
								20,
								1,
								0, 0, 0, 0, 0, 0, 0, 4,
								0, 4, 116, 101, 115, 116,

								0, 10,
								1,
								1,
								0, 0, 0, 0, 0, 0, 0, 38,
							}))
						})
					})

				})

			})

		})

	})

})
