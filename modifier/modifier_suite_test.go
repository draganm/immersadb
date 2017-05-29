package modifier_test

import (
	"fmt"
	"io"
	"io/ioutil"

	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/modifier"
	"github.com/draganm/immersadb/store"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestModifier(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Modifier Suite")
}

var _ = Describe("Modifier", func() {
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

	Describe("Type", func() {
		var t modifier.EntityType
		Context("When the data value exists", func() {
			BeforeEach(func() {
				Expect(m.CreateData(modifier.DBPath{"test"}, func(w io.Writer) error {
					_, e := w.Write([]byte("test"))
					return e
				})).To(Succeed())
				_, err = s.Append(chunk.NewCommitChunk(m.RootAddress))
				Expect(err).ToNot(HaveOccurred())

			})
			Context("When I get the type", func() {
				BeforeEach(func() {
					er, err := m.EntityReaderFor(modifier.DBPath{"test"})
					Expect(err).ToNot(HaveOccurred())
					t = er.Type()
				})
				It("Should return Data", func() {
					Expect(t).To(Equal(modifier.Data))
				})
			})
		})

		Context("When the hash value exists", func() {
			BeforeEach(func() {
				Expect(m.CreateHash(modifier.DBPath{"test"})).To(Succeed())
				_, err = s.Append(chunk.NewCommitChunk(m.RootAddress))
				Expect(err).ToNot(HaveOccurred())
			})
			Context("When I get the type", func() {
				BeforeEach(func() {
					er, err := m.EntityReaderFor(modifier.DBPath{"test"})
					Expect(err).ToNot(HaveOccurred())
					t = er.Type()
				})
				It("Should return Hash", func() {
					Expect(t).To(Equal(modifier.Hash))
				})
			})
		})

		Context("When the array value exists", func() {
			BeforeEach(func() {
				Expect(m.CreateArray(modifier.DBPath{"test"})).To(Succeed())
			})
			Context("When I get the type", func() {
				BeforeEach(func() {
					er, err := m.EntityReaderFor(modifier.DBPath{"test"})
					Expect(err).ToNot(HaveOccurred())
					t = er.Type()
				})
				It("Should return Array", func() {
					Expect(t).To(Equal(modifier.Array))
				})
			})
		})

	})

	Describe("ForEachHashEntry", func() {
		Context("When there is one value", func() {
			BeforeEach(func() {
				Expect(m.CreateData(modifier.DBPath{"test"}, func(w io.Writer) error {
					_, e := w.Write([]byte("testValue"))
					return e
				})).To(Succeed())
			})

			Context("When I iterate over all values", func() {
				var values map[string]string
				BeforeEach(func() {
					values = map[string]string{}
					err = m.ForEachHashEntry(func(key string, reader modifier.EntityReader) error {
						r, e := reader.Data()
						if e != nil {
							return e
						}
						d, e := ioutil.ReadAll(r)
						if e != nil {
							return e
						}
						values[key] = string(d)
						return nil
					})
				})

				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})

				It("Should read the values", func() {
					Expect(values).To(Equal(map[string]string{"test": "testValue"}))
				})

				Context("When I add another value", func() {
					BeforeEach(func() {
						Expect(m.CreateData(modifier.DBPath{"test2"}, func(w io.Writer) error {
							_, e := w.Write([]byte("testValue2"))
							return e
						})).To(Succeed())
					})
					Context("When I iterate over all values", func() {
						var values map[string]string
						BeforeEach(func() {
							values = map[string]string{}
							err = m.ForEachHashEntry(func(key string, reader modifier.EntityReader) error {
								r, e := reader.Data()
								if e != nil {
									return e
								}
								d, e := ioutil.ReadAll(r)
								if e != nil {
									return e
								}
								values[key] = string(d)
								return nil
							})
						})

						It("Should not return error", func() {
							Expect(err).ToNot(HaveOccurred())
						})

						It("Should read the values", func() {
							Expect(values).To(Equal(map[string]string{
								"test":  "testValue",
								"test2": "testValue2",
							}))
						})
					})

				})
			})
		})
	})

	Describe("Exists", func() {
		Context("When the value does not exist", func() {
			Context("When I check for existence", func() {
				var exists bool
				BeforeEach(func() {
					exists = m.Exists(modifier.DBPath{"test"})
				})
				It("Should return false", func() {
					Expect(exists).To(BeFalse())
				})
			})
		})
		Context("When the value exists", func() {
			BeforeEach(func() {
				Expect(m.CreateData(modifier.DBPath{"test"}, func(w io.Writer) error {
					_, e := w.Write([]byte("test"))
					return e
				})).To(Succeed())
			})
			Context("When I check for existence", func() {
				var exists bool
				BeforeEach(func() {
					exists = m.Exists(modifier.DBPath{"test"})
				})
				It("Should return true", func() {
					Expect(exists).To(BeTrue())
				})
			})
		})
	})

	Describe("GetData", func() {
		BeforeEach(func() {
			Expect(m.CreateData(modifier.DBPath{"test"}, func(w io.Writer) error {
				_, e := w.Write([]byte("test"))
				return e
			})).To(Succeed())
		})

		Context("When I get the value that exists", func() {
			var r io.Reader
			BeforeEach(func() {
				er, err := m.EntityReaderFor(modifier.DBPath{"test"})
				Expect(err).ToNot(HaveOccurred())
				r, err = er.Data()
			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should return non-null reader", func() {
				Expect(r).ToNot(BeNil())
			})

			Context("When I read the data from the reader", func() {
				var data []byte
				BeforeEach(func() {
					data, err = ioutil.ReadAll(r)
				})
				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})
				It("Should read the data", func() {
					Expect(string(data)).To(Equal("test"))
				})
			})

		})
	})

	Describe("CreateData", func() {

		Context("When there is a array", func() {
			BeforeEach(func() {
				Expect(m.CreateArray(modifier.DBPath{"l1"})).To(Succeed())
			})
			Context("When I append a value to the array head", func() {
				BeforeEach(func() {
					err = m.CreateData(modifier.DBPath{"l1", 0}, func(w io.Writer) error {
						_, e := w.Write([]byte("test-test-test"))
						return e
					})
				})
				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})
				Context("When I read the head of the array", func() {
					var r io.Reader
					BeforeEach(func() {
						er, err := m.EntityReaderFor(modifier.DBPath{"l1", 0})
						Expect(err).ToNot(HaveOccurred())
						r, err = er.Data()
					})
					It("Should not return error", func() {
						Expect(err).ToNot(HaveOccurred())
					})
					Context("When I read the value", func() {
						var data []byte
						BeforeEach(func() {
							data, err = ioutil.ReadAll(r)
						})
						It("Should not return error", func() {
							Expect(err).ToNot(HaveOccurred())
						})
						It("Should read the correct value", func() {
							Expect(string(data)).To(Equal("test-test-test"))
						})
					})
				})
				Context("When I append another value to the array head", func() {
					BeforeEach(func() {
						err = m.CreateData(modifier.DBPath{"l1", 0}, func(w io.Writer) error {
							_, e := w.Write([]byte("test-test-test2"))
							return e
						})
					})
					It("Should not return error", func() {
						Expect(err).ToNot(HaveOccurred())
					})
					Context("When I read the head of the array", func() {
						var r io.Reader
						BeforeEach(func() {
							er, err := m.EntityReaderFor(modifier.DBPath{"l1", 0})
							Expect(err).ToNot(HaveOccurred())
							r, err = er.Data()
						})
						It("Should not return error", func() {
							Expect(err).ToNot(HaveOccurred())
						})
						Context("When I read the value", func() {
							var data []byte
							BeforeEach(func() {
								data, err = ioutil.ReadAll(r)
							})
							It("Should not return error", func() {
								Expect(err).ToNot(HaveOccurred())
							})
							It("Should read the correct value", func() {
								Expect(string(data)).To(Equal("test-test-test2"))
							})
						})
					})
					Context("When I read the second value of the array", func() {
						var r io.Reader
						BeforeEach(func() {
							er, err := m.EntityReaderFor(modifier.DBPath{"l1", 1})
							Expect(err).ToNot(HaveOccurred())
							r, err = er.Data()
						})
						It("Should not return error", func() {
							Expect(err).ToNot(HaveOccurred())
						})
						Context("When I read the value", func() {
							var data []byte
							BeforeEach(func() {
								data, err = ioutil.ReadAll(r)
							})
							It("Should not return error", func() {
								Expect(err).ToNot(HaveOccurred())
							})
							It("Should read the correct value", func() {
								Expect(string(data)).To(Equal("test-test-test"))
							})
						})
					})
				})
			})
		})

		Context("When I create 16 value entries", func() {
			BeforeEach(func() {
				for i := 0; i < 16; i++ {
					err = m.CreateData(modifier.DBPath{fmt.Sprintf("test-%d", i)}, func(w io.Writer) error {
						_, e := w.Write([]byte("test-test-test"))
						return e
					})
					Expect(err).ToNot(HaveOccurred())
				}
				_, err = s.Append(chunk.NewCommitChunk(m.RootAddress))
				Expect(err).ToNot(HaveOccurred())

			})

			It("Should have Hash leaf as last chunk with 16 refs", func() {
				t, refs, _ := chunk.Parts(s.Chunk(chunk.LastCommitRootHashAddress(s)))
				Expect(t).To(Equal(chunk.HashLeafType))
				Expect(len(refs)).To(Equal(16))
			})

			Context("When I get one of the values", func() {
				var r io.Reader
				BeforeEach(func() {
					er, err := m.EntityReaderFor(modifier.DBPath{"test-1"})
					Expect(err).ToNot(HaveOccurred())
					r, err = er.Data()
				})
				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})

				Context("When I read the value", func() {
					var data []byte
					BeforeEach(func() {
						data, err = ioutil.ReadAll(r)
					})
					It("Should not return error", func() {
						Expect(err).ToNot(HaveOccurred())
					})
					It("Should read the correct value", func() {
						Expect(string(data)).To(Equal("test-test-test"))
					})
				})
			})

			Context("When I add one more entry", func() {
				BeforeEach(func() {
					err = m.CreateData(modifier.DBPath{"oops!oops!"}, func(w io.Writer) error {
						_, e := w.Write([]byte("test-test-test"))
						return e
					})
					Expect(err).ToNot(HaveOccurred())
					_, err = s.Append(chunk.NewCommitChunk(m.RootAddress))
					Expect(err).ToNot(HaveOccurred())
				})

				It("Should have Hash node", func() {
					t, _, _ := chunk.Parts(s.Chunk(chunk.LastCommitRootHashAddress(s)))
					Expect(t).To(Equal(chunk.HashNodeType))
				})

				Context("When I get one of the values", func() {
					var r io.Reader
					BeforeEach(func() {
						er, err := m.EntityReaderFor(modifier.DBPath{"test-15"})
						Expect(err).ToNot(HaveOccurred())
						r, err = er.Data()
					})
					It("Should not return error", func() {
						Expect(err).ToNot(HaveOccurred())
					})

					Context("When I read the value", func() {
						var data []byte
						BeforeEach(func() {
							data, err = ioutil.ReadAll(r)
						})
						It("Should not return error", func() {
							Expect(err).ToNot(HaveOccurred())
						})
						It("Should read the correct value", func() {
							Expect(string(data)).To(Equal("test-test-test"))
						})
					})
				})

				Context("When I add another entry", func() {
					BeforeEach(func() {
						err = m.CreateData(modifier.DBPath{"oops!oops!"}, func(w io.Writer) error {
							_, e := w.Write([]byte("test-test-test"))
							return e
						})
					})
					It("Should not return error", func() {
						Expect(err).ToNot(HaveOccurred())
					})

					Context("When I get one of the values", func() {
						var r io.Reader
						BeforeEach(func() {
							er, err := m.EntityReaderFor(modifier.DBPath{"oops!oops!"})
							Expect(err).ToNot(HaveOccurred())
							r, err = er.Data()
						})
						It("Should not return error", func() {
							Expect(err).ToNot(HaveOccurred())
						})

						Context("When I read the value", func() {
							var data []byte
							BeforeEach(func() {
								data, err = ioutil.ReadAll(r)
							})
							It("Should not return error", func() {
								Expect(err).ToNot(HaveOccurred())
							})
							It("Should read the correct value", func() {
								Expect(string(data)).To(Equal("test-test-test"))
							})
						})
					})

				})

			})

		})

		Context("When the path has only one string entry", func() {
			BeforeEach(func() {
				err = m.CreateData(modifier.DBPath{"test"}, func(w io.Writer) error {
					_, e := w.Write([]byte{1, 2, 3, 4})
					return e
				})
				_, err = s.Append(chunk.NewCommitChunk(m.RootAddress))
				Expect(err).ToNot(HaveOccurred())

			})

			It("Shoud not return an error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should create two more chunks", func() {
				Expect(s.Data()).To(Equal([]byte{
					// old root
					0, 0, 0, 4,
					0, 20,
					0, 0,
					0, 0, 0, 4,

					// old commit
					0, 0, 0, 12,
					0, 1,
					0, 1,
					0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 12,

					// Data chunk
					0, 0, 0, 8,
					0, 10,
					0, 0,
					1, 2, 3, 4,
					0, 0, 0, 8,

					// Data header chunk
					0, 0, 0, 20,
					0, 11,
					0, 1,
					0, 0, 0, 0, 0, 0, 0, 32,

					// size
					0, 0, 0, 0, 0, 0, 0, 4,
					0, 0, 0, 20,

					// New root
					0, 0, 0, 18,
					0, 20,
					0, 1,
					0, 0, 0, 0, 0, 0, 0, 48,
					0, 4, 116, 101, 115, 116,
					0, 0, 0, 18,

					// New commit
					0, 0, 0, 12,
					0, 1,
					0, 1,
					0, 0, 0, 0, 0, 0, 0, 76,
					0, 0, 0, 12,
				}))
			})

		})
	})

	Describe("CreateArray", func() {
		Context("When the path has only one string entry", func() {
			BeforeEach(func() {
				err = m.CreateArray(modifier.DBPath{"test"})
			})

			It("Shoud not return an error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should create two more chunks", func() {
				Expect(s.Data()).To(Equal([]byte{

					// old root
					0, 0, 0, 4,
					0, 20,
					0, 0,
					0, 0, 0, 4,

					// old commit
					0, 0, 0, 12,
					0, 1,
					0, 1,
					0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 12,

					// empty array
					0, 0, 0, 4,
					0, 30,
					0, 0,
					0, 0, 0, 4,

					// new root
					0, 0, 0, 18,
					0, 20,
					0, 1,
					0, 0, 0, 0, 0, 0, 0, 32,
					// 'test'
					0, 4, 116, 101, 115, 116,
					0, 0, 0, 18,
				}))
			})

		})
	})

	Describe("CreateHash", func() {

		Context("When the path has only one string entry", func() {
			BeforeEach(func() {
				err = m.CreateHash(modifier.DBPath{"test"})
			})

			It("Shoud not return an error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should create two more chunks", func() {
				Expect(s.Data()).To(Equal([]byte{
					// old root
					0, 0, 0, 4,
					0, 20,
					0, 0,
					0, 0, 0, 4,

					// old commit
					0, 0, 0, 12,
					0, 1,
					0, 1,
					0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 12,

					// new leaf
					0, 0, 0, 4,
					0, 20,
					0, 0,
					0, 0, 0, 4,

					// new root
					0, 0, 0, 18,
					0, 20,
					// refs
					0, 1,
					// ref to new leaf
					0, 0, 0, 0, 0, 0, 0, 32,
					// 'test'
					0, 4, 116, 101, 115, 116,
					0, 0, 0, 18,
				}))
			})

			Context("When I create a nested hash", func() {
				BeforeEach(func() {
					err = m.CreateHash(modifier.DBPath{"test", "test2"})
				})
				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("When I create another at the root level", func() {
				BeforeEach(func() {
					err = m.CreateHash(modifier.DBPath{"test2"})
				})

				It("Shoud not return an error", func() {
					Expect(err).ToNot(HaveOccurred())
				})

				It("Should create two more chunks", func() {
					Expect(s.Data()).To(Equal([]byte{
						// initial root
						0, 0, 0, 4,
						0, 20,
						0, 0,
						0, 0, 0, 4,

						// Old commit

						0, 0, 0, 12,
						0, 1,
						0, 1,
						0, 0, 0, 0, 0, 0, 0, 0,
						0, 0, 0, 12,

						// new leaf
						0, 0, 0, 4,
						0, 20,
						0, 0,
						0, 0, 0, 4,

						// previous root
						0, 0, 0, 18,
						0, 20,
						// refs
						0, 1,
						// ref to new leaf
						0, 0, 0, 0, 0, 0, 0, 32,
						// 'test'
						0, 4, 116, 101, 115, 116,
						0, 0, 0, 18,

						// empty test2 hash
						0, 0, 0, 4,
						0, 20,
						0, 0,
						0, 0, 0, 4,

						// new root
						0, 0, 0, 33,
						0, 20,

						// 2 refs
						0, 2,
						0, 0, 0, 0, 0, 0, 0, 32,
						0, 0, 0, 0, 0, 0, 0, 70,

						// test
						0, 4, 116, 101, 115, 116,
						// test2
						0, 5, 116, 101, 115, 116, 50,

						0, 0, 0, 33,
					}))
				})

			})

		})
	})

})
