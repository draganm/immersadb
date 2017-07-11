package modifier_test

import (
	"io"
	"io/ioutil"

	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/dbpath"
	"github.com/draganm/immersadb/modifier"
	"github.com/draganm/immersadb/modifier/ttfmap"
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
		s = store.NewMemoryStore(nil)

		_, err = ttfmap.CreateEmpty(s)
		Expect(err).ToNot(HaveOccurred())

		_, err = s.Append(chunk.NewCommitChunk(0))
		Expect(err).ToNot(HaveOccurred())
		m = modifier.New(s, 8192, chunk.LastCommitRootHashAddress(s))
	})

	Describe("Type", func() {
		var t modifier.EntityType
		Context("When the data value exists", func() {
			BeforeEach(func() {
				Expect(m.CreateData(dbpath.Path{"test"}, func(w io.Writer) error {
					_, e := w.Write([]byte("test"))
					return e
				})).To(Succeed())
				_, err = s.Append(chunk.NewCommitChunk(m.RootAddress))
				Expect(err).ToNot(HaveOccurred())

			})
			Context("When I get the type", func() {
				BeforeEach(func() {
					er := m.EntityReaderFor(dbpath.Path{"test"})
					t = er.Type()
				})
				It("Should return Data", func() {
					Expect(t).To(Equal(modifier.Data))
				})
			})
		})

		Context("When the hash value exists", func() {
			BeforeEach(func() {
				Expect(m.CreateMap(dbpath.Path{"test"})).To(Succeed())
				_, err = s.Append(chunk.NewCommitChunk(m.RootAddress))
				Expect(err).ToNot(HaveOccurred())
			})
			Context("When I get the type", func() {
				BeforeEach(func() {
					er := m.EntityReaderFor(dbpath.Path{"test"})
					t = er.Type()
				})
				It("Should return Map", func() {
					Expect(t).To(Equal(modifier.Map))
				})
			})
		})

		Context("When the array value exists", func() {
			BeforeEach(func() {
				Expect(m.CreateArray(dbpath.Path{"test"})).To(Succeed())
			})
			Context("When I get the type", func() {
				BeforeEach(func() {
					er := m.EntityReaderFor(dbpath.Path{"test"})
					t = er.Type()
				})
				It("Should return Array", func() {
					Expect(t).To(Equal(modifier.Array))
				})
			})
		})

	})

	Describe("ForEachMapEntry", func() {
		Context("When there is one value", func() {
			BeforeEach(func() {
				Expect(m.CreateData(dbpath.Path{"test"}, func(w io.Writer) error {
					_, e := w.Write([]byte("testValue"))
					return e
				})).To(Succeed())
			})

			Context("When I iterate over all values", func() {
				var values map[string]string
				BeforeEach(func() {
					values = map[string]string{}
					err = m.ForEachMapEntry(func(key string, reader modifier.EntityReader) error {
						r := reader.Data()
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
						Expect(m.CreateData(dbpath.Path{"test2"}, func(w io.Writer) error {
							_, e := w.Write([]byte("testValue2"))
							return e
						})).To(Succeed())
					})
					Context("When I iterate over all values", func() {
						var values map[string]string
						BeforeEach(func() {
							values = map[string]string{}
							err = m.ForEachMapEntry(func(key string, reader modifier.EntityReader) error {
								r := reader.Data()
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
					exists = m.Exists(dbpath.Path{"test"})
				})
				It("Should return false", func() {
					Expect(exists).To(BeFalse())
				})
			})
		})
		Context("When the value exists", func() {
			BeforeEach(func() {
				Expect(m.CreateData(dbpath.Path{"test"}, func(w io.Writer) error {
					_, e := w.Write([]byte("test"))
					return e
				})).To(Succeed())
			})
			Context("When I check for existence", func() {
				var exists bool
				BeforeEach(func() {
					exists = m.Exists(dbpath.Path{"test"})
				})
				It("Should return true", func() {
					Expect(exists).To(BeTrue())
				})
			})
		})
	})

	Describe("GetData", func() {
		BeforeEach(func() {
			Expect(m.CreateData(dbpath.Path{"test"}, func(w io.Writer) error {
				_, e := w.Write([]byte("test"))
				return e
			})).To(Succeed())
		})

		Context("When I get the value that exists", func() {
			var r io.Reader
			BeforeEach(func() {
				er := m.EntityReaderFor(dbpath.Path{"test"})
				r = er.Data()
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
				Expect(m.CreateArray(dbpath.Path{"l1"})).To(Succeed())
			})
			Context("When I append a value to the array head", func() {
				BeforeEach(func() {
					err = m.CreateData(dbpath.Path{"l1", 0}, func(w io.Writer) error {
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
						er := m.EntityReaderFor(dbpath.Path{"l1", 0})
						r = er.Data()
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
						err = m.CreateData(dbpath.Path{"l1", 0}, func(w io.Writer) error {
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
							er := m.EntityReaderFor(dbpath.Path{"l1", 0})
							r = er.Data()
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
							er := m.EntityReaderFor(dbpath.Path{"l1", 1})
							r = er.Data()
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
				err = m.CreateData(dbpath.Path{"test"}, func(w io.Writer) error {
					_, e := w.Write([]byte{1, 2, 3, 4})
					return e
				})
				_, err = s.Append(chunk.NewCommitChunk(m.RootAddress))
				Expect(err).ToNot(HaveOccurred())

			})

			It("Shoud not return an error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

		})
	})

	Describe("CreateArray", func() {
		Context("When the path has only one string entry", func() {
			BeforeEach(func() {
				err = m.CreateArray(dbpath.Path{"test"})
			})

			It("Shoud not return an error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

		})
	})

	Describe("CreateMap", func() {

		Context("When the path has only one string entry", func() {
			BeforeEach(func() {
				err = m.CreateMap(dbpath.Path{"test"})
			})

			It("Shoud not return an error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			Context("When I create a nested hash", func() {
				BeforeEach(func() {
					err = m.CreateMap(dbpath.Path{"test", "test2"})
				})
				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})
			})

			Context("When I create another at the root level", func() {
				BeforeEach(func() {
					err = m.CreateMap(dbpath.Path{"test2"})
				})

				It("Shoud not return an error", func() {
					Expect(err).ToNot(HaveOccurred())
				})

			})

		})
	})

})
