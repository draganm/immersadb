package modifier_test

import (
	"fmt"
	"io"

	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/modifier"
	"github.com/draganm/immersadb/store"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Modifier.Size", func() {
	var s *store.MemoryStore
	var m *modifier.Modifier
	var err error
	BeforeEach(func() {
		s = store.NewMemoryStore([]byte{
			// Hash Root Chunk
			0, 0, 0, 4,
			//
			0, 20,
			0, 0,
			0, 0, 0, 4,
		})
		_, err = s.Append(chunk.NewCommitChunk(0))
		Expect(err).ToNot(HaveOccurred())

		m = modifier.New(s, 8192, chunk.LastCommitRootHashAddress(s))
	})

	Context("Hash", func() {
		BeforeEach(func() {
			Expect(m.CreateHash(modifier.DBPath{"test"})).To(Succeed())
			_, err = s.Append(chunk.NewCommitChunk(m.RootAddress))
			Expect(err).ToNot(HaveOccurred())

		})
		var s uint64
		JustBeforeEach(func() {
			er, err := m.EntityReaderFor(modifier.DBPath{"test"})
			Expect(err).ToNot(HaveOccurred())
			s = er.Size()
		})
		Context("When the Hash is empty", func() {

			It("Should return 0", func() {
				Expect(s).To(Equal(uint64(0)))
			})

		})

		Context("When the Hash has one element", func() {

			BeforeEach(func() {
				Expect(m.CreateHash(modifier.DBPath{"test", "e1"})).To(Succeed())
			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should return 1", func() {
				Expect(s).To(Equal(uint64(1)))
			})

		})

		Context("When the Hash 17 elements", func() {

			BeforeEach(func() {
				for i := 0; i < 17; i++ {
					Expect(m.CreateHash(modifier.DBPath{"test", fmt.Sprintf("test-%d", i)})).To(Succeed())
				}
			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should return 17", func() {
				Expect(s).To(Equal(uint64(17)))
			})

		})

	})

	Context("List", func() {
		BeforeEach(func() {
			Expect(m.CreateArray(modifier.DBPath{"test"})).To(Succeed())
		})
		var s uint64
		JustBeforeEach(func() {
			er, err := m.EntityReaderFor(modifier.DBPath{"test"})
			Expect(err).ToNot(HaveOccurred())
			s = er.Size()
		})
		Context("When the List is empty", func() {

			It("Should return 0", func() {
				Expect(s).To(Equal(uint64(0)))
			})

		})

		Context("When the List has one element", func() {

			BeforeEach(func() {
				Expect(m.CreateHash(modifier.DBPath{"test", 0})).To(Succeed())
			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should return 1", func() {
				Expect(s).To(Equal(uint64(1)))
			})

		})

		Context("When the List has 17 elements", func() {

			BeforeEach(func() {
				for i := 0; i < 17; i++ {
					Expect(m.CreateHash(modifier.DBPath{"test", 0})).To(Succeed())
				}
			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should return 17", func() {
				Expect(s).To(Equal(uint64(17)))
			})

		})

		Context("When the List has 1024 elements", func() {

			BeforeEach(func() {
				for i := 0; i < 1024; i++ {
					Expect(m.CreateHash(modifier.DBPath{"test", 0})).To(Succeed())
				}
			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should return 1024", func() {
				Expect(s).To(Equal(uint64(1024)))
			})

		})

	})

	Context("Data", func() {
		var s uint64
		JustBeforeEach(func() {
			er, err := m.EntityReaderFor(modifier.DBPath{"test"})
			Expect(err).ToNot(HaveOccurred())
			s = er.Size()
		})
		Context("When the Data is empty", func() {
			BeforeEach(func() {
				Expect(m.CreateData(modifier.DBPath{"test"}, func(w io.Writer) error {
					_, e := w.Write([]byte{})
					return e
				})).To(Succeed())
			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should return 0", func() {
				Expect(s).To(Equal(uint64(0)))
			})

		})

		Context("When the Data has one byte", func() {
			BeforeEach(func() {
				Expect(m.CreateData(modifier.DBPath{"test"}, func(w io.Writer) error {
					_, e := w.Write([]byte{1})
					return e
				})).To(Succeed())
			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should return 1", func() {
				Expect(s).To(Equal(uint64(1)))
			})

		})

		Context("When the Data has 17 elements", func() {

			BeforeEach(func() {
				Expect(m.CreateData(modifier.DBPath{"test"}, func(w io.Writer) error {
					_, e := w.Write(make([]byte, 17))
					return e
				})).To(Succeed())
			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should return 17", func() {
				Expect(s).To(Equal(uint64(17)))
			})

		})

	})

})
