package modifier_test

import (
	"io"
	"io/ioutil"
	"testing"

	"github.com/draganm/immersadb/modifier"
	"github.com/draganm/immersadb/store"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestImmersadb(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Modifier Suite")
}

var _ = Describe("DataReader", func() {
	var s *store.MemoryStore
	BeforeEach(func() {
		s = store.NewMemoryStore(nil)
	})

	Describe("NewDataReader", func() {
		var fileChunkAddr uint64

		var read []byte
		var err error
		var r *modifier.DataReader
		var bytesRead int

		Context("When there is a single data chunk file in the store", func() {
			BeforeEach(func() {
				r, err = modifier.NewDataReader(store.NewMemoryStore([]byte{
					0, 14,
					// type
					10,
					// refs
					0,
					// data
					0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,

					0, 6,
					// type
					10,
					// refs
					0,
					// data
					12, 13, 14, 15,

					0, 18,
					//type
					11,
					// refs
					2,
					0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 0, 0, 0, 0, 16,
					//

				}), 24)
				Expect(err).ToNot(HaveOccurred())
			})

			Context("When I read all the data", func() {
				BeforeEach(func() {
					read, err = ioutil.ReadAll(r)
				})
				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})
				It("Should read the data", func() {
					Expect(read).To(Equal([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}))
				})
			})

			Context("When I read the data from the first chunk bar one byte", func() {
				BeforeEach(func() {
					read = make([]byte, 11)
					bytesRead, err = r.Read(read)
				})

				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})

				It("Should read 11 bytes", func() {
					Expect(bytesRead).To(Equal(11))
				})

				It("Should read the data", func() {
					Expect(read).To(Equal([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}))
				})

				Context("When I try to read next 10 bytes", func() {
					BeforeEach(func() {
						read = make([]byte, 10)
						bytesRead, err = r.Read(read)
					})

					It("Should not return error", func() {
						Expect(err).ToNot(HaveOccurred())
					})

					It("Should read 1 byte", func() {
						Expect(bytesRead).To(Equal(1))
					})

					It("Should read the data", func() {
						Expect(read).To(Equal([]byte{11, 0, 0, 0, 0, 0, 0, 0, 0, 0}))
					})

					Context("When I try to read further 10 bytes", func() {
						BeforeEach(func() {
							read = make([]byte, 10)
							bytesRead, err = r.Read(read)
						})

						It("Should not return error", func() {
							Expect(err).ToNot(HaveOccurred())
						})

						It("Should read 4 bytes", func() {
							Expect(bytesRead).To(Equal(4))
						})

						It("Should read the data", func() {
							Expect(read).To(Equal([]byte{12, 13, 14, 15, 0, 0, 0, 0, 0, 0}))
						})

						Context("When I try to read another byte", func() {
							BeforeEach(func() {
								read = make([]byte, 1)
								bytesRead, err = r.Read(read)
							})

							It("Should return io.EOF error", func() {
								Expect(err).To(Equal(io.EOF))
							})

							It("Should read 0 bytes", func() {
								Expect(bytesRead).To(Equal(0))
							})

							It("Should not read the data", func() {
								Expect(read).To(Equal([]byte{0}))
							})
						})

						Context("When I try to read one more byte", func() {
							BeforeEach(func() {
								read = make([]byte, 1)
								bytesRead, err = r.Read(read)
							})

							It("Should return io.EOF error", func() {
								Expect(err).To(Equal(io.EOF))
							})

							It("Should read 0 bytes", func() {
								Expect(bytesRead).To(Equal(0))
							})

							It("Should not read the data", func() {
								Expect(read).To(Equal([]byte{0}))
							})
						})

					})
				})

			})

		})

		Context("When there is a single data chunk file in the store", func() {

			BeforeEach(func() {
				w := modifier.NewDataWriter(s, 16)
				_, err = w.Write([]byte{1, 2})
				Expect(err).ToNot(HaveOccurred())
				fileChunkAddr, err = w.Close()
				Expect(err).ToNot(HaveOccurred())
				r, err = modifier.NewDataReader(s, fileChunkAddr)
				Expect(err).ToNot(HaveOccurred())
			})

			Context("When I read the first byte", func() {
				BeforeEach(func() {
					read = []byte{0}
					bytesRead, err = r.Read(read)
				})

				It("Should read one byte", func() {
					Expect(bytesRead).To(Equal(1))
				})

				It("Should not return an error", func() {
					Expect(err).ToNot(HaveOccurred())
				})

				It("Should populate the value", func() {
					Expect(read).To(Equal([]byte{1}))
				})

				Context("When I read the second byte", func() {
					BeforeEach(func() {
						read = []byte{0}
						bytesRead, err = r.Read(read)
					})
					It("Should read one byte", func() {
						Expect(bytesRead).To(Equal(1))
					})

					It("Should not return an error", func() {
						Expect(err).ToNot(HaveOccurred())
					})

					It("Should populate the value", func() {
						Expect(read).To(Equal([]byte{2}))
					})

					Context("When I try reading one more byte", func() {
						BeforeEach(func() {
							read = []byte{0}
							bytesRead, err = r.Read(read)
						})

						It("Should read zero bytes", func() {
							Expect(bytesRead).To(Equal(0))
						})

						It("Should return io.EOF error", func() {
							Expect(err).To(Equal(io.EOF))
						})

						It("Should not populate the value", func() {
							Expect(read).To(Equal([]byte{0}))
						})

					})

				})
			})
		})

	})

})
