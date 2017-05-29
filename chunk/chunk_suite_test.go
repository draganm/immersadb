package chunk_test

import (
	"github.com/draganm/immersadb/chunk"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestChunk(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Chunk Suite")
}

var _ = Describe("Pack()", func() {
	var t chunk.ChunkType
	var refs []uint64
	var d []byte
	Context("When type is Data", func() {
		BeforeEach(func() {
			t = chunk.DataType
		})
		Context("When refs and data are both nil", func() {
			It("Creates 4 byte chunk", func() {
				Expect(chunk.Pack(t, refs, d)).To(Equal([]byte{0, 10, 0, 0}))
			})
		})

		Context("When refs is an empty array and data is nil", func() {
			BeforeEach(func() {
				refs = []uint64{}
			})
			It("Creates 4 byte chunk", func() {
				Expect(chunk.Pack(t, refs, d)).To(Equal([]byte{0, 10, 0, 0}))
			})
		})

		Context("When refs is an empty array and data is empty array", func() {
			BeforeEach(func() {
				refs = []uint64{}
				d = []byte{}
			})
			It("Creates 4 byte chunk", func() {
				Expect(chunk.Pack(t, refs, d)).To(Equal([]byte{0, 10, 0, 0}))
			})
		})

		Context("When refs has one reference and data is nil", func() {
			BeforeEach(func() {
				refs = []uint64{1}
				d = nil
			})
			It("Creates 12 byte chunk", func() {
				Expect(chunk.Pack(t, refs, d)).To(Equal([]byte{0, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1}))
			})
		})

		Context("When refs has one reference and data is empty array", func() {
			BeforeEach(func() {
				refs = []uint64{1}
				d = []byte{}
			})
			It("Creates 12 byte chunk", func() {
				Expect(chunk.Pack(t, refs, d)).To(Equal([]byte{0, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1}))
			})
		})

		Context("When refs is nil and data has one byte", func() {
			BeforeEach(func() {
				refs = nil
				d = []byte{42}
			})
			It("Creates 5 byte chunk", func() {
				Expect(chunk.Pack(t, refs, d)).To(Equal([]byte{0, 10, 0, 0, 42}))
			})
		})

		Context("When refs is empty array and data has one byte", func() {
			BeforeEach(func() {
				refs = []uint64{}
				d = []byte{42}
			})
			It("Creates 5 byte chunk", func() {
				Expect(chunk.Pack(t, refs, d)).To(Equal([]byte{0, 10, 0, 0, 42}))
			})
		})

		Context("When refs has one ref and data has one byte", func() {
			BeforeEach(func() {
				refs = []uint64{1}
				d = []byte{42}
			})
			It("Creates 13 byte chunk", func() {
				Expect(chunk.Pack(t, refs, d)).To(Equal([]byte{0, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 42}))
			})
		})

	})

})

var _ = Describe("Parts()", func() {
	var t chunk.ChunkType
	var refs []uint64
	var d []byte

	Context("When chunk is nil", func() {
		BeforeEach(func() {
			t, refs, d = chunk.Parts(nil)
		})

		It("Should return error type", func() {
			Expect(t).To(Equal(chunk.ErrorType))
		})

		It("Should return nil refs", func() {
			Expect(refs).To(BeNil())
		})

		It("Should return nil data", func() {
			Expect(d).To(BeNil())
		})

	})

	Context("When chunk is only one byte long", func() {
		BeforeEach(func() {
			t, refs, d = chunk.Parts([]byte{0})
		})

		It("Should return error type", func() {
			Expect(t).To(Equal(chunk.ErrorType))
		})

		It("Should return nil refs", func() {
			Expect(refs).To(BeNil())
		})

		It("Should return nil data", func() {
			Expect(d).To(BeNil())
		})

	})

	Context("When chunk is only 2 bytes long and type Data", func() {
		BeforeEach(func() {
			t, refs, d = chunk.Parts([]byte{0, 10})
		})

		It("Should return data type", func() {
			Expect(t).To(Equal(chunk.DataType))
		})

		It("Should return nil refs", func() {
			Expect(refs).To(BeNil())
		})

		It("Should return nil data", func() {
			Expect(d).To(BeNil())
		})

	})

	Context("When chunk is only 4 bytes long and has Data type and 0 references", func() {
		BeforeEach(func() {
			t, refs, d = chunk.Parts([]byte{0, 10, 0, 0})
		})

		It("Should return data type", func() {
			Expect(t).To(Equal(chunk.DataType))
		})

		It("Should return empty refs slice", func() {
			Expect(refs).To(Equal([]uint64{}))
		})

		It("Should return empty data", func() {
			Expect(d).To(Equal([]byte{}))
		})

	})

	Context("When chunk is 5 bytes long and has Data type and 1 references", func() {
		BeforeEach(func() {
			t, refs, d = chunk.Parts([]byte{0, 1, 0, 1, 255})
		})

		It("Should return error type", func() {
			Expect(t).To(Equal(chunk.ErrorType))
		})

		It("Should return nil refs", func() {
			Expect(refs).To(BeNil())
		})

		It("Should return nil data", func() {
			Expect(d).To(BeNil())
		})

	})

	Context("When chunk is 12 bytes long and has Data type and 1 references", func() {
		BeforeEach(func() {
			t, refs, d = chunk.Parts([]byte{0, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1})
		})

		It("Should return error type", func() {
			Expect(t).To(Equal(chunk.DataType))
		})

		It("Should return one ref", func() {
			Expect(refs).To(Equal([]uint64{1}))
		})

		It("Should return empty data", func() {
			Expect(d).To(Equal([]byte{}))
		})

	})

})
