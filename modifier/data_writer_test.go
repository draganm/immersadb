package modifier_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/draganm/immersadb/modifier"
	"github.com/draganm/immersadb/store"
)

var _ = Describe("DataWriter", func() {
	var s *store.MemoryStore
	BeforeEach(func() {
		s = store.NewMemoryStore(nil)
	})

	Context("When I write 1 byte", func() {
		var written int
		var err error
		var w *modifier.DataWriter
		BeforeEach(func() {
			w = modifier.NewDataWriter(s, 16)
			written, err = w.Write([]byte{1})
		})

		It("Should not return error", func() {
			Expect(err).ToNot(HaveOccurred())
		})

		It("Should return 1 byte written", func() {
			Expect(written).To(Equal(1))
		})

		Context("When I close the writer", func() {
			var ref uint64
			BeforeEach(func() {
				ref, err = w.Close()
			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should return address of the written segment", func() {
				Expect(ref).To(Equal(uint64(13)))
			})

			It("Should have written segments to storage", func() {
				Expect(s.Data()).To(Equal([]byte{
					0, 0, 0, 5,
					0, 1,

					0, 0,

					1,
					0, 0, 0, 5,

					0, 0, 0, 20,
					0, 2,

					0, 1,
					0, 0, 0, 0, 0, 0, 0, 0,

					0, 0, 0, 0, 0, 0, 0, 1,

					0, 0, 0, 20,
				}))
			})
		})

	})

	Context("When I write more data than single chunk can take", func() {
		var written int
		var err error
		var w *modifier.DataWriter
		BeforeEach(func() {
			w = modifier.NewDataWriter(s, 16)
			data := make([]byte, 16)
			for i := range data {
				data[i] = byte(i)
			}
			written, err = w.Write(data)
		})

		It("Should not return error", func() {
			Expect(err).ToNot(HaveOccurred())
		})

		It("Should return 16 bytes written", func() {
			Expect(written).To(Equal(16))
		})

		Context("When I close the writer", func() {
			var ref uint64
			BeforeEach(func() {
				ref, err = w.Close()
			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should return address of the written segment", func() {
				Expect(ref).To(Equal(uint64(40)))
			})

			It("Should have written segments to storage", func() {
				Expect(s.Data()).To(Equal([]byte{
					0, 0, 0, 16,
					// type
					0, 1,
					// refs
					0, 0,
					// data
					0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
					0, 0, 0, 16,
					// chunk end

					0, 0, 0, 8,
					// type
					0, 1,
					// refs
					0, 0,
					// data
					12, 13, 14, 15,
					0, 0, 0, 8,
					// chunk end

					0, 0, 0, 28,
					//type
					0, 2,
					// refs
					0, 2,
					0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 0, 0, 0, 0, 24,
					//

					0, 0, 0, 0, 0, 0, 0, 16,

					0, 0, 0, 28,
				}))
			})

		})
	})

	Context("When I write more data than two chunks can take", func() {
		var written int
		var err error
		var w *modifier.DataWriter
		BeforeEach(func() {
			w = modifier.NewDataWriter(s, 20)
			data := make([]byte, 40)
			for i := range data {
				data[i] = byte(i)
			}
			written, err = w.Write(data)
		})

		It("Should not return error", func() {
			Expect(err).ToNot(HaveOccurred())
		})

		It("Should return 40 bytes written", func() {
			Expect(written).To(Equal(40))
		})

		Context("When I close the writer", func() {
			var ref uint64
			BeforeEach(func() {
				ref, err = w.Close()
			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should return address of the written segment", func() {
				Expect(ref).To(Equal(uint64(76)))
			})

			It("Should have written segments to storage", func() {
				Expect(s.Data()).To(Equal([]byte{
					// data chunk
					0, 0, 0, 20,
					0, 1,
					0, 0,
					0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
					0, 0, 0, 20,

					// data chunk
					0, 0, 0, 20,
					0, 1,
					0, 0,
					16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
					0, 0, 0, 20,

					// data chunk
					0, 0, 0, 12,
					0, 1,
					0, 0,
					32, 33, 34, 35, 36, 37, 38, 39,
					0, 0, 0, 12,

					// data header chunk
					0, 0, 0, 36,
					0, 2,
					0, 3,
					0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 0, 0, 0, 0, 28,
					0, 0, 0, 0, 0, 0, 0, 56,

					// size
					0, 0, 0, 0, 0, 0, 0, 40,

					0, 0, 0, 36,
				}))
			})

		})
	})

	Context("When I write more data than can be written into file", func() {
		var written int
		var err error
		var w *modifier.DataWriter
		BeforeEach(func() {
			w = modifier.NewDataWriter(s, 16)
			data := make([]byte, 1024)
			for i := range data {
				data[i] = byte(i)
			}
			written, err = w.Write(data)
		})

		It("Should return error", func() {
			Expect(err).To(Equal(modifier.ErrDataTooLarge))
		})

	})

})
