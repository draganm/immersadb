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

	BeforeEach(func() {
		Expect(i.Transaction(func(m modifier.MapWriter) error {
			return m.CreateArray("test", nil)
		})).To(Succeed())
	})

	Describe("InMap", func() {

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
	Describe("PrependArray", func() {
		var funcExecuted bool
		JustBeforeEach(func() {
			funcExecuted = false
			Expect(i.Transaction(func(m modifier.MapWriter) error {
				return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
					return m.PrependArray(func(m modifier.ArrayWriter) error {
						funcExecuted = true
						return nil
					})
				})
			})).To(Succeed())
		})

		Context("When array is empty", func() {
			It("Should execute the passed function", func() {
				Expect(funcExecuted).To(BeTrue())
			})
			It("Should prepend value of type Array", func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.InArray("test", func(m modifier.ArrayReader) error {
						Expect(m.Size()).To(Equal(uint64(1)))
						Expect(m.Type(0)).To(Equal(modifier.Array))
						return nil
					})
				})).To(Succeed())
			})
		})

		Context("When array has one element", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
						return m.PrependMap(nil)
					})
				})).To(Succeed())
			})
			It("Should execute the passed function", func() {
				Expect(funcExecuted).To(BeTrue())
			})
			It("Should prepend value of type Array", func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.InArray("test", func(m modifier.ArrayReader) error {
						Expect(m.Size()).To(Equal(uint64(2)))
						Expect(m.Type(0)).To(Equal(modifier.Array))
						return nil
					})
				})).To(Succeed())
			})
		})

	})

	Describe("ModifyArray", func() {
		var err error
		var funcExecuted bool
		JustBeforeEach(func() {
			funcExecuted = false
			err = i.Transaction(func(m modifier.MapWriter) error {
				return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
					return m.ModifyArray(0, func(m modifier.ArrayWriter) error {
						funcExecuted = true
						return nil
					})
				})
			})
		})

		Context("When array with the index does not exist", func() {
			It("Should return modifier.ErrIndexOutOfBounds error", func() {
				Expect(err).To(Equal(modifier.ErrIndexOutOfBounds))
			})
		})

		Context("When element is not an array", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
						return m.PrependMap(nil)
					})
				})).To(Succeed())
			})
			It("Should return modifier.ErrNotArray", func() {
				Expect(err).To(Equal(modifier.ErrNotArray))
			})
		})

		Context("When element is an array", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
						return m.PrependArray(nil)
					})
				})).To(Succeed())
			})
			It("Should not return an error", func() {
				Expect(err).To(Succeed())
			})
			It("Should execute the passed function", func() {
				Expect(funcExecuted).To(BeTrue())
			})
		})

	})

	Describe("PrependMap", func() {
		var funcExecuted bool
		JustBeforeEach(func() {
			funcExecuted = false
			Expect(i.Transaction(func(m modifier.MapWriter) error {
				return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
					return m.PrependMap(func(m modifier.MapWriter) error {
						funcExecuted = true
						return nil
					})
				})
			})).To(Succeed())
		})

		Context("When array is empty", func() {
			It("Should execute the passed function", func() {
				Expect(funcExecuted).To(BeTrue())
			})
			It("Should prepend value of type Map", func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.InArray("test", func(m modifier.ArrayReader) error {
						Expect(m.Size()).To(Equal(uint64(1)))
						Expect(m.Type(0)).To(Equal(modifier.Map))
						return nil
					})
				})).To(Succeed())
			})
		})

		Context("When array has one element", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
						return m.PrependArray(nil)
					})
				})).To(Succeed())
			})
			It("Should execute the passed function", func() {
				Expect(funcExecuted).To(BeTrue())
			})
			It("Should prepend value of type Map", func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.InArray("test", func(m modifier.ArrayReader) error {
						Expect(m.Size()).To(Equal(uint64(2)))
						Expect(m.Type(0)).To(Equal(modifier.Map))
						return nil
					})
				})).To(Succeed())
			})
		})

	})

	Describe("ModifyMap", func() {
		var err error
		var funcExecuted bool
		JustBeforeEach(func() {
			funcExecuted = false
			err = i.Transaction(func(m modifier.MapWriter) error {
				return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
					return m.ModifyMap(0, func(m modifier.MapWriter) error {
						funcExecuted = true
						return nil
					})
				})
			})
		})

		Context("When map with the index does not exist", func() {
			It("Should return modifier.ErrIndexOutOfBounds error", func() {
				Expect(err).To(Equal(modifier.ErrIndexOutOfBounds))
			})
		})

		Context("When element is not a map", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
						return m.PrependArray(nil)
					})
				})).To(Succeed())
			})
			It("Should return modifier.ErrNotMap", func() {
				Expect(err).To(Equal(modifier.ErrNotMap))
			})
		})

		Context("When element is a map", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
						return m.PrependMap(nil)
					})
				})).To(Succeed())
			})
			It("Should not return an error", func() {
				Expect(err).To(Succeed())
			})
			It("Should execute the passed function", func() {
				Expect(funcExecuted).To(BeTrue())
			})
		})

	})

	Describe("PrependData", func() {
		JustBeforeEach(func() {
			Expect(i.Transaction(func(m modifier.MapWriter) error {
				return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
					return m.PrependData(func(w io.Writer) error {
						_, e := w.Write([]byte{1, 2, 3})
						return e
					})
				})
			})).To(Succeed())
		})

		Context("When array is empty", func() {
			It("Should prepend value of type Data", func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.InArray("test", func(m modifier.ArrayReader) error {
						Expect(m.Size()).To(Equal(uint64(1)))
						Expect(m.Type(0)).To(Equal(modifier.Data))
						return nil
					})
				})).To(Succeed())
			})
		})

		Context("When array has one element", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
						return m.PrependArray(nil)
					})
				})).To(Succeed())
			})
			It("Should prepend value of type Map", func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.InArray("test", func(m modifier.ArrayReader) error {
						Expect(m.Size()).To(Equal(uint64(2)))
						Expect(m.Type(0)).To(Equal(modifier.Data))
						return nil
					})
				})).To(Succeed())
			})
		})

	})

	Describe("SetData", func() {
		var err error
		var funcExecuted bool
		JustBeforeEach(func() {
			funcExecuted = false
			err = i.Transaction(func(m modifier.MapWriter) error {
				return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
					return m.SetData(0, func(w io.Writer) error {
						_, e := w.Write([]byte{1, 2, 3})
						return e
					})
				})
			})
		})

		Context("When element with the index does not exist", func() {
			It("Should return modifier.ErrIndexOutOfBounds error", func() {
				Expect(err).To(Equal(modifier.ErrIndexOutOfBounds))
			})
		})

		Context("When element is not data", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
						return m.PrependArray(nil)
					})
				})).To(Succeed())
			})
			It("Should not return an error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should change the type to data", func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.InArray("test", func(m modifier.ArrayReader) error {
						Expect(m.Type(0)).To(Equal(modifier.Data))
						return nil
					})
				})).To(Succeed())
			})

		})

		Context("When element is data", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
						return m.PrependData(func(w io.Writer) error {
							_, e := w.Write([]byte{3, 2, 1})
							return e
						})
					})
				})).To(Succeed())
			})
			It("Should not return an error", func() {
				Expect(err).To(Succeed())
			})
			It("Should change the data", func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.InArray("test", func(m modifier.ArrayReader) error {
						return m.ReadData(0, func(r io.Reader) error {
							d, e := ioutil.ReadAll(r)
							Expect(d).To(Equal([]byte{1, 2, 3}))
							return e
						})
					})
				})).To(Succeed())
			})
		})

	})

	Describe("DeleteLast", func() {
		var err error
		JustBeforeEach(func() {
			err = i.Transaction(func(m modifier.MapWriter) error {
				return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
					return m.DeleteLast()
				})
			})
		})
		Context("When array is empty", func() {
			It("Should return modifier.ErrArrayEmpty error", func() {
				Expect(err).To(Equal(modifier.ErrArrayEmpty))
			})
		})
		Context("When array is not empty", func() {
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

			It("Should remove last value", func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.InArray("test", func(m modifier.ArrayReader) error {
						Expect(m.Size()).To(Equal(uint64(0)))
						return nil
					})
				})).To(Succeed())
			})
		})

	})

	Describe("DeleteAll", func() {
		JustBeforeEach(func() {
			Expect(i.Transaction(func(m modifier.MapWriter) error {
				return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
					return m.DeleteAll()
				})
			})).To(Succeed())
		})

		Context("When array is empty", func() {
			It("Should keep the array empty", func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.InArray("test", func(m modifier.ArrayReader) error {
						Expect(m.Size()).To(Equal(uint64(0)))
						return nil
					})
				})).To(Succeed())
			})
		})
		Context("When array is not empty", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.ModifyArray("test", func(m modifier.ArrayWriter) error {
						return m.PrependMap(nil)
					})
				})).To(Succeed())
			})
			It("Should clear all values for the array", func() {
				Expect(i.Transaction(func(m modifier.MapWriter) error {
					return m.InArray("test", func(m modifier.ArrayReader) error {
						Expect(m.Size()).To(Equal(uint64(0)))
						return nil
					})
				})).To(Succeed())
			})
		})
	})

})
