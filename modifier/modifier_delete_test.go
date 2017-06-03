package modifier_test

import (
	"fmt"
	"io"

	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/gc"
	"github.com/draganm/immersadb/modifier"
	"github.com/draganm/immersadb/store"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Modifier.Delete", func() {
	var s *store.MemoryStore
	var m *modifier.Modifier
	var err error
	BeforeEach(func() {
		s = store.NewMemoryStore([]byte{
			// Hash Root Chunk
			0, 2,
			20,
			0,
		})
		_, err = s.Append(chunk.NewCommitChunk(0))
		Expect(err).ToNot(HaveOccurred())
		m = modifier.New(s, 8192, chunk.LastCommitRootHashAddress(s))
	})

	Context("Hash", func() {

		JustBeforeEach(func() {
			err = m.Delete(modifier.DBPath{"test"})
			_, err = s.Append(chunk.NewCommitChunk(m.RootAddress))
			Expect(err).ToNot(HaveOccurred())
		})

		Context("When the Hash has the element", func() {
			BeforeEach(func() {
				Expect(m.CreateHash(modifier.DBPath{"test"})).To(Succeed())
				_, err = s.Append(chunk.NewCommitChunk(m.RootAddress))
				Expect(err).ToNot(HaveOccurred())

			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should reduce hash size to 0", func() {
				Expect(m.Size()).To(Equal(uint64(0)))
			})
		})

		Context("When the Hash 18 elements", func() {

			BeforeEach(func() {
				Expect(m.CreateHash(modifier.DBPath{"test"})).To(Succeed())
				_, err = s.Append(chunk.NewCommitChunk(m.RootAddress))
				Expect(err).ToNot(HaveOccurred())
			})

			BeforeEach(func() {
				for i := 0; i < 17; i++ {
					Expect(m.CreateHash(modifier.DBPath{fmt.Sprintf("test-%d", i)})).To(Succeed())
				}
				_, err = s.Append(chunk.NewCommitChunk(m.RootAddress))
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should reduce hash size to 17", func() {
				Expect(m.Size()).To(Equal(uint64(17)))
			})

		})

	})

	Context("Array", func() {
		BeforeEach(func() {
			Expect(m.CreateArray(modifier.DBPath{"test"})).To(Succeed())
			_, err = s.Append(chunk.NewCommitChunk(m.RootAddress))
			Expect(err).ToNot(HaveOccurred())
		})

		Context("When I create a 17 element array", func() {
			BeforeEach(func() {
				for i := 0; i < 17; i++ {
					Expect(m.CreateHash(modifier.DBPath{"test", 0})).To(Succeed())
				}
				_, err = s.Append(chunk.NewCommitChunk(m.RootAddress))
				Expect(err).ToNot(HaveOccurred())

			})
			It("Should have created it", func() {
				Expect(compactStore(s)).To(Equal([]byte{
					0, 2,
					30,
					0,

					0, 2,
					20,
					0,

					0, 10,
					30,
					1,
					0, 0, 0, 0, 0, 0, 0, 4,

					0, 34,
					30,
					4,
					0, 0, 0, 0, 0, 0, 0, 4,
					0, 0, 0, 0, 0, 0, 0, 4,
					0, 0, 0, 0, 0, 0, 0, 4,
					0, 0, 0, 0, 0, 0, 0, 4,

					0, 34,
					30,
					4,
					0, 0, 0, 0, 0, 0, 0, 4,
					0, 0, 0, 0, 0, 0, 0, 4,
					0, 0, 0, 0, 0, 0, 0, 4,
					0, 0, 0, 0, 0, 0, 0, 4,

					0, 34,
					30,
					4,
					0, 0, 0, 0, 0, 0, 0, 4,
					0, 0, 0, 0, 0, 0, 0, 4,
					0, 0, 0, 0, 0, 0, 0, 4,
					0, 0, 0, 0, 0, 0, 0, 4,

					0, 34,
					30,
					4,
					0, 0, 0, 0, 0, 0, 0, 4,
					0, 0, 0, 0, 0, 0, 0, 4,
					0, 0, 0, 0, 0, 0, 0, 4,
					0, 0, 0, 0, 0, 0, 0, 4,

					0, 68,
					31,
					4,
					0, 0, 0, 0, 0, 0, 0, 20,
					0, 0, 0, 0, 0, 0, 0, 56,
					0, 0, 0, 0, 0, 0, 0, 92,
					0, 0, 0, 0, 0, 0, 0, 128,
					0, 1,
					0, 0, 0, 0, 0, 0, 0, 4,
					0, 0, 0, 0, 0, 0, 0, 4,
					0, 0, 0, 0, 0, 0, 0, 4,
					0, 0, 0, 0, 0, 0, 0, 4,

					0, 68,
					31,
					4,
					0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 0, 0, 0, 0, 8,
					0, 0, 0, 0, 0, 0, 0, 164,
					0, 2,
					0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 0, 0, 0, 0, 1,
					0, 0, 0, 0, 0, 0, 0, 16,

					0, 16,
					20,
					1,
					0, 0, 0, 0, 0, 0, 0, 234,
					0, 4, 116, 101, 115, 116,

					0, 10,
					1,
					1,
					0, 0, 0, 0, 0, 0, 1, 48,
				}))
			})

			Context("When I rotate 16 values in the array", func() {
				BeforeEach(func() {
					for i := 0; i < 16; i++ {
						Expect(m.CreateHash(modifier.DBPath{"test", 0})).To(Succeed())
						Expect(m.Delete(modifier.DBPath{"test", 16})).To(Succeed())
					}
					_, err = s.Append(chunk.NewCommitChunk(m.RootAddress))
					Expect(err).ToNot(HaveOccurred())

				})

				It("Should have created it", func() {
					Expect(compactStore(s)).To(Equal([]byte{

						0, 2,
						30,
						0,

						0, 2,
						20,
						0,

						0, 10,
						30,
						1,
						0, 0, 0, 0, 0, 0, 0, 4,

						0, 34,
						30,
						4,
						0, 0, 0, 0, 0, 0, 0, 4,
						0, 0, 0, 0, 0, 0, 0, 4,
						0, 0, 0, 0, 0, 0, 0, 4,
						0, 0, 0, 0, 0, 0, 0, 4,

						0, 68,
						31,
						4,
						0, 0, 0, 0, 0, 0, 0, 0,
						0, 0, 0, 0, 0, 0, 0, 0,
						0, 0, 0, 0, 0, 0, 0, 8,
						0, 0, 0, 0, 0, 0, 0, 20,
						0, 1,
						0, 0, 0, 0, 0, 0, 0, 0,
						0, 0, 0, 0, 0, 0, 0, 0,
						0, 0, 0, 0, 0, 0, 0, 1,
						0, 0, 0, 0, 0, 0, 0, 4,

						0, 34,
						30,
						4,
						0, 0, 0, 0, 0, 0, 0, 4,
						0, 0, 0, 0, 0, 0, 0, 4,
						0, 0, 0, 0, 0, 0, 0, 4,
						0, 0, 0, 0, 0, 0, 0, 4,

						0, 34,
						30,
						4,
						0, 0, 0, 0, 0, 0, 0, 4,
						0, 0, 0, 0, 0, 0, 0, 4,
						0, 0, 0, 0, 0, 0, 0, 4,
						0, 0, 0, 0, 0, 0, 0, 4,

						0, 26,
						30,
						3,
						0, 0, 0, 0, 0, 0, 0, 4,
						0, 0, 0, 0, 0, 0, 0, 4,
						0, 0, 0, 0, 0, 0, 0, 4,

						0, 10,
						30,
						1,
						0, 0, 0, 0, 0, 0, 0, 4,

						0, 68,
						31,
						4,
						0, 0, 0, 0, 0, 0, 0, 0,
						0, 0, 0, 0, 0, 0, 0, 0,
						0, 0, 0, 0, 0, 0, 0, 0,
						0, 0, 0, 0, 0, 0, 0, 226,
						0, 1,
						0, 0, 0, 0, 0, 0, 0, 0,
						0, 0, 0, 0, 0, 0, 0, 0,
						0, 0, 0, 0, 0, 0, 0, 0,
						0, 0, 0, 0, 0, 0, 0, 1,

						0, 68,
						31,
						4,
						0, 0, 0, 0, 0, 0, 0, 126,
						0, 0, 0, 0, 0, 0, 0, 162,
						0, 0, 0, 0, 0, 0, 0, 198,
						0, 0, 0, 0, 0, 0, 0, 238,
						0, 2,
						0, 0, 0, 0, 0, 0, 0, 4,
						0, 0, 0, 0, 0, 0, 0, 4,
						0, 0, 0, 0, 0, 0, 0, 3,
						0, 0, 0, 0, 0, 0, 0, 1,

						0, 68,
						31,
						4,
						0, 0, 0, 0, 0, 0, 0, 0,
						0, 0, 0, 0, 0, 0, 0, 0,
						0, 0, 0, 0, 0, 0, 0, 56,
						0, 0, 0, 0, 0, 0, 1, 52,
						0, 3,
						0, 0, 0, 0, 0, 0, 0, 0,
						0, 0, 0, 0, 0, 0, 0, 0,
						0, 0, 0, 0, 0, 0, 0, 5,
						0, 0, 0, 0, 0, 0, 0, 12,

						0, 16,
						20,
						1,
						0, 0, 0, 0, 0, 0, 1, 122,
						0, 4, 116, 101, 115, 116,

						0, 10,
						1,
						1,
						0, 0, 0, 0, 0, 0, 1, 192,
					}))
				})

			})
		})

		Context("Deleting first element", func() {

			JustBeforeEach(func() {
				err = m.Delete(modifier.DBPath{"test", 0})
			})

			Context("When the Array has one element", func() {

				BeforeEach(func() {
					Expect(m.CreateHash(modifier.DBPath{"test", 0})).To(Succeed())
				})

				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})

				It("Should reduce the size of list to 0", func() {
					er, err := m.EntityReaderFor(modifier.DBPath{"test"})
					Expect(err).ToNot(HaveOccurred())
					Expect(er.Size()).To(Equal(uint64(0)))
				})

			})

			Context("When the Array has 17 elements", func() {

				BeforeEach(func() {
					for i := 0; i < 17; i++ {
						Expect(m.CreateHash(modifier.DBPath{"test", 0})).To(Succeed())
					}
				})

				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})

				It("Should return 16", func() {
					er, err := m.EntityReaderFor(modifier.DBPath{"test"})
					Expect(err).ToNot(HaveOccurred())
					Expect(er.Size()).To(Equal(uint64(16)))
				})

			})

			Context("When the Array has 1024 elements", func() {

				BeforeEach(func() {
					for i := 0; i < 1024; i++ {
						Expect(m.CreateHash(modifier.DBPath{"test", 0})).To(Succeed())
					}
				})

				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})

				It("Should return 1024", func() {
					er, err := m.EntityReaderFor(modifier.DBPath{"test"})
					Expect(err).ToNot(HaveOccurred())
					Expect(er.Size()).To(Equal(uint64(1023)))
				})

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

func compactStore(s store.Store) []byte {
	d := store.NewMemoryStore(nil)
	Expect(gc.Copy(s, d)).To(Succeed())
	return d.Data()
}
