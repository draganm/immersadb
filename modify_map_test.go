package immersadb_test

import (
	"io"
	"io/ioutil"
	"os"

	"github.com/draganm/immersadb"
	"github.com/draganm/immersadb/modifier"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Modify Map", func() {

	var i *immersadb.ImmersaDB
	var err error

	var dir string
	BeforeEach(func() {
		dir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())
		i, err = immersadb.New(dir)
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		if dir != "" {
			Expect(os.RemoveAll(dir)).To(Succeed())
		}
	})

	Describe("DeleteAll", func() {
		var err error

		Context("When deleting sub-map", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.CreateMap("test", nil)
				})).To(Succeed())
			})
			JustBeforeEach(func() {
				err = i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyMap("test", func(m modifier.MapWriter) error {
						return m.DeleteAll()
					})

				})
			})

			Context("When the map is empty", func() {
				It("Should not return error", func() {
					Expect(err).To(Succeed())
				})
			})

			Context("When a key exists", func() {
				BeforeEach(func() {
					Expect(i.Transaction(func(m modifier.MapWriter) error {
						return m.ModifyMap("test", func(m modifier.MapWriter) error {
							return m.CreateMap("subtest", nil)
						})
					}))
				})
				It("Should not return error", func() {
					Expect(err).To(BeNil())
				})
				It("Should delete the key", func() {
					exists := true
					Expect(i.Transaction(func(m modifier.MapWriter) error {
						return m.InMap("test", func(m modifier.MapReader) error {
							exists = m.HasKey("subtest")
							return nil
						})
					}))
					Expect(exists).To(BeFalse())
				})
				It("Should not delete the parent map", func() {
					exists := false
					Expect(i.Transaction(func(m modifier.MapWriter) error {
						exists = m.HasKey("test")
						return nil
					}))
					Expect(exists).To(BeTrue())

				})
			})
		})

		Context("When deleting root map", func() {

			JustBeforeEach(func() {
				err = i.Transaction(func(m modifier.MapWriter) error {
					return m.DeleteAll()
				})
			})

			Context("When the map is empty", func() {
				It("Should not return error", func() {
					Expect(err).To(Succeed())
				})
			})

			Context("When a key exists", func() {
				BeforeEach(func() {
					Expect(i.Transaction(func(m modifier.MapWriter) error {
						return m.CreateMap("test", nil)
					}))
				})
				It("Should not return error", func() {
					Expect(err).To(BeNil())
				})
				It("Should delete the key", func() {
					exists := true
					Expect(i.Transaction(func(m modifier.MapWriter) error {
						exists = m.HasKey("test")
						return nil
					}))
					Expect(exists).To(BeFalse())
				})
			})
		})

	})

	Describe("ReadData", func() {
		var read []byte
		var err error
		JustBeforeEach(func() {
			read = nil
			err = i.Transaction(func(w modifier.MapWriter) error {
				return w.ReadData("test", func(r io.Reader) error {
					d, e := ioutil.ReadAll(r)
					read = d
					return e
				})
			})
		})

		Context("When data does not exist", func() {
			It("Should return modifier.ErrKeyDoesNotExist", func() {
				Expect(err).To(Equal(modifier.ErrKeyDoesNotExist))
			})
		})

		Context("When the key is not data", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.CreateMap("test", nil)
				})).To(Succeed())
			})

			It("Should return modifier.ErrNotData error", func() {
				Expect(err).To(Equal(modifier.ErrNotData))
			})
		})

		Context("When data exists", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.SetData("test", func(w io.Writer) error {
						_, e := w.Write([]byte{1, 2, 3})
						return e
					})
				})).To(Succeed())
			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should read the data", func() {
				Expect(read).To(Equal([]byte{1, 2, 3}))
			})
		})

	})

	Describe("InArray", func() {
		var executed bool
		var err error
		JustBeforeEach(func() {
			executed = false
			err = i.Transaction(func(m modifier.MapWriter) error {
				return m.InArray("test", func(m modifier.ArrayReader) error {
					executed = true
					return nil
				})
			})
		})

		Context("When key exists and is an array", func() {
			BeforeEach(func() {
				Expect(
					i.Transaction(func(m modifier.MapWriter) error {
						return m.CreateArray("test", nil)
					})).To(Succeed())
			})
			It("Should execute the passed function", func() {
				Expect(executed).To(BeTrue())
			})
			It("Should not return an error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("When key exists and is not an array", func() {
			BeforeEach(func() {
				Expect(
					i.Transaction(func(m modifier.MapWriter) error {
						return m.CreateMap("test", nil)
					})).To(Succeed())
			})
			It("Should not execute the passed function", func() {
				Expect(executed).To(BeFalse())
			})
			It("Should not return modifier.ErrNotArray error", func() {
				Expect(err).To(Equal(modifier.ErrNotArray))
			})
		})

		Context("When key does not exist", func() {
			It("Should not execute the passed function", func() {
				Expect(executed).To(BeFalse())
			})
			It("Should not return modifier.ErrKeyDoesNotExist error", func() {
				Expect(err).To(Equal(modifier.ErrKeyDoesNotExist))
			})
		})

	})

	Describe("InMap", func() {
		var executed bool
		var err error
		JustBeforeEach(func() {
			executed = false
			err = i.Transaction(func(m modifier.MapWriter) error {
				return m.InMap("test", func(m modifier.MapReader) error {
					executed = true
					return nil
				})
			})
		})

		Context("When key exists and is a map", func() {
			BeforeEach(func() {
				Expect(
					i.Transaction(func(m modifier.MapWriter) error {
						return m.CreateMap("test", nil)
					})).To(Succeed())
			})
			It("Should execute the passed function", func() {
				Expect(executed).To(BeTrue())
			})
			It("Should not return an error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("When key exists and is not a map", func() {
			BeforeEach(func() {
				Expect(
					i.Transaction(func(m modifier.MapWriter) error {
						return m.CreateArray("test", nil)
					})).To(Succeed())
			})
			It("Should not execute the passed function", func() {
				Expect(executed).To(BeFalse())
			})
			It("Should not return modifier.ErrNotMap error", func() {
				Expect(err).To(Equal(modifier.ErrNotMap))
			})
		})

		Context("When key does not exist", func() {
			It("Should not execute the passed function", func() {
				Expect(executed).To(BeFalse())
			})
			It("Should not return modifier.ErrKeyDoesNotExist error", func() {
				Expect(err).To(Equal(modifier.ErrKeyDoesNotExist))
			})
		})

	})

	Describe("DeleteKey", func() {
		var err error
		JustBeforeEach(func() {
			err = i.Transaction(func(m modifier.MapWriter) error {
				return m.DeleteKey("test")
			})
		})

		Context("When the key does not exist", func() {
			It("Should return modifier.ErrKeyDoesNotExist error", func() {
				Expect(err).To(Equal(modifier.ErrKeyDoesNotExist))
			})
		})

		Context("When the key exists", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.CreateMap("test", nil)
				}))
			})
			It("Should not return error", func() {
				Expect(err).To(BeNil())
			})
			It("Should delete the key", func() {
				exists := true
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					exists = m.HasKey("test")
					return nil
				}))
				Expect(exists).To(BeFalse())
			})
		})

	})

	Describe("ModifyMap", func() {
		var err error
		JustBeforeEach(func() {
			err = i.Transaction(func(m modifier.MapWriter) error {
				return m.ModifyMap("test", func(m modifier.MapWriter) error {
					return m.CreateMap("subtest", nil)
				})
			})
		})
		Context("When the map does not exist", func() {
			It("Should return modifier.ErrKeyDoesNotExist error", func() {
				Expect(err).To(Equal(modifier.ErrKeyDoesNotExist))
			})
		})
		Context("When the map exists", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.CreateMap("test", nil)
				})).To(Succeed())
			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should execute the modification", func() {
				var found bool
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.InMap("test", func(m modifier.MapReader) error {
						found = m.HasKey("subtest")
						return nil
					})
				})).To(Succeed())
				Expect(found).To(BeTrue())
			})
		})
		Context("When the key not a map", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.CreateArray("test", nil)
				})).To(Succeed())
			})

			It("Should return error modifier.ErrNotMap", func() {
				Expect(err).To(Equal(modifier.ErrNotMap))
			})
		})
	})

	Describe("ForEach", func() {

		type KeyType struct {
			Key  string
			Type modifier.EntityType
		}
		var keys []KeyType

		JustBeforeEach(func() {
			keys = nil
			Expect(i.Transaction(func(m modifier.MapWriter) error {
				return m.ForEach(func(key string, t modifier.EntityType) error {
					keys = append(keys, KeyType{key, t})
					return nil
				})
			})).To(Succeed())
		})

		Context("When map is empty", func() {
			It("Should not be called", func() {
				Expect(keys).To(BeNil())
			})
		})

		Context("When map has one element", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.CreateMap("test", nil)
				})).To(Succeed())
			})
			It("should iterate over that key", func() {
				Expect(keys).To(Equal([]KeyType{{Key: "test", Type: modifier.Map}}))
			})
		})

		Context("When map has two elements", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					err := m.CreateMap("test", nil)
					if err != nil {
						return err
					}
					return m.CreateArray("test1", nil)
				})).To(Succeed())
			})
			It("should iterate over both keys", func() {
				Expect(keys).To(Equal([]KeyType{
					{Key: "test", Type: modifier.Map},
					{Key: "test1", Type: modifier.Array},
				}))
			})
		})

	})

	Describe("HasKey", func() {
		var hasKey bool

		JustBeforeEach(func() {
			Expect(i.Transaction(func(m modifier.MapWriter) error {
				hasKey = m.HasKey("test")
				return nil
			})).To(Succeed())
		})

		Context("When key does not exist", func() {
			It("should return false", func() {
				Expect(hasKey).To(BeFalse())
			})
		})

		Context("When key exists exist", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(w modifier.MapWriter) error {
					return w.CreateArray("test", nil)
				})).To(Succeed())
			})

			It("should return true", func() {
				Expect(hasKey).To(BeTrue())
			})
		})

	})

})
