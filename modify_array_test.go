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

var _ = Describe("Modify Array", func() {

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

	Describe("InMap", func() {
		BeforeEach(func() {
			Expect(i.Transaction(func(m modifier.MapWriter) error {
				return m.CreateArray("test", nil)
			})).To(Succeed())
		})
		var err error
		var executed bool
		JustBeforeEach(func() {
			executed = false
			err = i.Transaction(func(m modifier.MapWriter) error {
				return m.InArray("test", func(m modifier.ArrayReader) error {
					return m.InMap(0, func(m modifier.MapReader) error {
						executed = true
						return nil
					})
				})
			})
		})

		Context("When array is empty", func() {
			It("Should return error modifier.ErrIndexOutOfBounds", func() {
				Expect(err).To(Equal(modifier.ErrIndexOutOfBounds))
			})
			It("Should not execute passed function", func() {
				Expect(executed).To(BeFalse())
			})

		})

		Context("When index exists and has type map", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
						return m.PrependMap(nil)
					})
				})).To(Succeed())
			})
			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should execute the passed function", func() {
				Expect(executed).To(BeTrue())
			})
		})

		Context("When index exists and not have type map", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
						return m.PrependArray(nil)
					})
				})).To(Succeed())
			})
			It("Should return error modifier.ErrNotMap", func() {
				Expect(err).To(Equal(modifier.ErrNotMap))
			})
			It("Should not execute passed function", func() {
				Expect(executed).To(BeFalse())
			})
		})

	})

	Describe("InArray", func() {
		BeforeEach(func() {
			Expect(i.Transaction(func(m modifier.MapWriter) error {
				return m.CreateArray("test", nil)
			})).To(Succeed())
		})
		var err error
		var executed bool
		JustBeforeEach(func() {
			executed = false
			err = i.Transaction(func(m modifier.MapWriter) error {
				return m.InArray("test", func(m modifier.ArrayReader) error {
					return m.InArray(0, func(m modifier.ArrayReader) error {
						executed = true
						return nil
					})
				})
			})
		})

		Context("When parent array is empty", func() {
			It("Should return error modifier.ErrIndexOutOfBounds", func() {
				Expect(err).To(Equal(modifier.ErrIndexOutOfBounds))
			})
			It("Should not execute passed function", func() {
				Expect(executed).To(BeFalse())
			})

		})

		Context("When index exists and has type array", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
						return m.PrependArray(nil)
					})
				})).To(Succeed())
			})
			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should execute the passed function", func() {
				Expect(executed).To(BeTrue())
			})
		})

		Context("When index exists and not have type array", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
						return m.PrependMap(nil)
					})
				})).To(Succeed())
			})
			It("Should return error modifier.ErrNotArray", func() {
				Expect(err).To(Equal(modifier.ErrNotArray))
			})
			It("Should not execute passed function", func() {
				Expect(executed).To(BeFalse())
			})
		})

	})

	Describe("ReadData", func() {
		BeforeEach(func() {
			Expect(i.Transaction(func(m modifier.MapWriter) error {
				return m.CreateArray("test", nil)
			})).To(Succeed())
		})

		var err error
		var read []byte
		JustBeforeEach(func() {
			read = nil
			err = i.Transaction(func(m modifier.MapWriter) error {
				return m.InArray("test", func(m modifier.ArrayReader) error {
					return m.ReadData(0, func(r io.Reader) error {
						rr, e := ioutil.ReadAll(r)
						read = rr
						return e
					})
				})
			})
		})

		Context("When array does not contain the index", func() {
			It("Should return modifier.ErrIndexOutOfBounds error", func() {
				Expect(err).To(Equal(modifier.ErrIndexOutOfBounds))
			})
		})

		Context("When the value is not of the type data", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
						return m.PrependMap(nil)
					})
				})).To(Succeed())
			})

			It("Should return modifier.ErrNotData error", func() {
				Expect(err).To(Equal(modifier.ErrNotData))
			})

		})

		Context("When the value is of the type data", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
						return m.PrependData(func(w io.Writer) error {
							_, e := w.Write([]byte{1, 2, 3})
							return e
						})
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

	Describe("ForEach", func() {
		BeforeEach(func() {
			Expect(i.Transaction(func(m modifier.MapWriter) error {
				return m.CreateArray("test", nil)
			})).To(Succeed())
		})

		var err error
		type indexAndType struct {
			Index uint64
			Type  modifier.EntityType
		}
		var iterated []indexAndType
		JustBeforeEach(func() {
			iterated = nil
			err = i.Transaction(func(m modifier.MapWriter) error {
				return m.InArray("test", func(m modifier.ArrayReader) error {
					return m.ForEach(func(index uint64, t modifier.EntityType) error {
						iterated = append(iterated, indexAndType{index, t})
						return nil
					})
				})
			})
		})

		Context("When array is empty", func() {
			It("Should iterate over values 0 times", func() {
				Expect(len(iterated)).To(Equal(0))
			})
			It("Should not return an error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("When array has one map", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
						return m.PrependMap(nil)
					})
				})).To(Succeed())

			})
			It("Should iterate over the map value", func() {
				Expect(iterated).To(Equal([]indexAndType{{Index: 0, Type: modifier.Map}}))
			})
			It("Should not return an error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("When array has one data", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
						return m.PrependData(func(w io.Writer) error {
							_, e := w.Write([]byte{1, 2, 3})
							return e
						})
					})
				})).To(Succeed())

			})
			It("Should iterate over the data value", func() {
				Expect(iterated).To(Equal([]indexAndType{{Index: 0, Type: modifier.Data}}))
			})
			It("Should not return an error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("When array has one array", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
						return m.PrependArray(nil)
					})
				})).To(Succeed())

			})
			It("Should iterate over the array value", func() {
				Expect(iterated).To(Equal([]indexAndType{{Index: 0, Type: modifier.Array}}))
			})
			It("Should not return an error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
		})

	})

	Describe("Type", func() {
		var t modifier.EntityType
		BeforeEach(func() {
			Expect(i.Transaction(func(m modifier.MapWriter) error {
				return m.CreateArray("test", nil)
			})).To(Succeed())
		})

		JustBeforeEach(func() {
			Expect(i.Transaction(func(m modifier.MapWriter) error {
				return m.InArray("test", func(m modifier.ArrayReader) error {
					t = m.Type(0)
					return nil
				})
			})).To(Succeed())
		})

		Context("When array does not contain the index", func() {
			It("Should return modifier.Unknown", func() {
				Expect(t).To(Equal(modifier.Unknown))
			})
		})
		Context("The element is a map", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
						return m.PrependMap(nil)
					})
				})).To(Succeed())
			})
			It("Should return modifier.Map", func() {
				Expect(t).To(Equal(modifier.Map))
			})
		})
		Context("The element is na array", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
						return m.PrependArray(nil)
					})
				})).To(Succeed())
			})
			It("Should return modifier.Array", func() {
				Expect(t).To(Equal(modifier.Array))
			})
		})
		Context("The element is data", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
						return m.PrependData(func(w io.Writer) error {
							_, e := w.Write([]byte{1, 2, 3})
							return e
						})
					})
				})).To(Succeed())
			})
			It("Should return modifier.Data", func() {
				Expect(t).To(Equal(modifier.Data))
			})
		})
	})

	Describe("Size", func() {

		BeforeEach(func() {
			Expect(i.Transaction(func(m modifier.MapWriter) error {
				return m.CreateArray("test", nil)
			})).To(Succeed())
		})

		var s uint64
		JustBeforeEach(func() {
			Expect(i.Transaction(func(m modifier.MapWriter) error {
				return m.InArray("test", func(m modifier.ArrayReader) error {
					s = m.Size()
					return nil
				})
			})).To(Succeed())
		})

		Context("When the array is empty", func() {
			It("Should return 0", func() {
				Expect(s).To(Equal(uint64(0)))
			})
		})
		Context("When the array has one element", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
						return m.PrependMap(nil)
					})
				})).To(Succeed())
			})
			It("Should return 1", func() {
				Expect(s).To(Equal(uint64(1)))
			})
		})
	})

})
