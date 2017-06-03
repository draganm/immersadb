package immersadb_test

import (
	"io"
	"io/ioutil"
	"os"
	"path"

	"github.com/draganm/immersadb"
	"github.com/draganm/immersadb/modifier"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/vmihailenco/msgpack.v2"
)

var _ = Describe("ImmersaDB: array round robin", func() {

	var i *immersadb.ImmersaDB
	var err error

	var dir string
	var dbFile string
	BeforeEach(func() {
		dir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())
		dbFile = path.Join(dir, "db.data")
		i, err = immersadb.New(dbFile, 128*1024)
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		if dir != "" {
			Expect(os.RemoveAll(dir)).To(Succeed())
		}
	})

	Context("When I create an array", func() {
		BeforeEach(func() {
			err := i.Transaction(func(m modifier.EntityWriter) error {
				return m.CreateArray(modifier.DBPath{"ar"})
			})
			Expect(err).ToNot(HaveOccurred())
		})
		Context("When I prepend 5 values to the array in 5 transactions", func() {
			BeforeEach(func() {
				for j := 0; j < 5; j++ {
					err := i.Transaction(func(m modifier.EntityWriter) error {
						return m.CreateData(modifier.DBPath{"ar", 0}, func(w io.Writer) error {
							return msgpack.NewEncoder(w).Encode(j)
						})
					})
					Expect(err).ToNot(HaveOccurred())
				}
			})

			Context("When I iterate over the values", func() {
				var elements []int
				JustBeforeEach(func() {
					elements = nil
					err := i.ReadTransaction(func(er modifier.EntityReader) error {
						ser, err := er.EntityReaderFor(modifier.DBPath{"ar"})
						if err != nil {
							return err
						}
						return ser.ForEachArrayElement(func(index uint64, reader modifier.EntityReader) error {
							var el int
							r, err := reader.Data()
							if err != nil {
								return err
							}
							err = msgpack.NewDecoder(r).Decode(&el)
							if err != nil {
								return err
							}
							elements = append(elements, el)
							return nil
						})
					})
					Expect(err).ToNot(HaveOccurred())
				})

				It("should contain all 5 values", func() {
					Expect(elements).To(Equal([]int{4, 3, 2, 1, 0}))
				})

				Context("When I prepend new and delete last element", func() {
					BeforeEach(func() {
						err := i.Transaction(func(m modifier.EntityWriter) error {

							err := m.CreateData(modifier.DBPath{"ar", 0}, func(w io.Writer) error {
								return msgpack.NewEncoder(w).Encode(5)
							})

							err = m.Delete(modifier.DBPath{"ar", 5})
							if err != nil {
								return err
							}

							return nil
						})
						Expect(err).ToNot(HaveOccurred())

					})
					It("should contain all 5 values", func() {
						Expect(elements).To(Equal([]int{5, 4, 3, 2, 1}))
					})
					It("Should have Size 4", func() {
						var size uint64
						err := i.ReadTransaction(func(er modifier.EntityReader) error {
							ser, err := er.EntityReaderFor(modifier.DBPath{"ar"})
							if err != nil {
								return err
							}
							size = ser.Size()
							return nil
						})
						Expect(err).ToNot(HaveOccurred())
						Expect(size).To(Equal(uint64(5)))
					})

					Context("When I rotate one more element", func() {
						BeforeEach(func() {
							err := i.Transaction(func(m modifier.EntityWriter) error {

								err := m.CreateData(modifier.DBPath{"ar", 0}, func(w io.Writer) error {
									return msgpack.NewEncoder(w).Encode(6)
								})

								if err != nil {
									return err
								}

								err = m.Delete(modifier.DBPath{"ar", 5})
								if err != nil {
									return err
								}

								return nil
							})
							Expect(err).ToNot(HaveOccurred())
						})
						It("should contain all 5 values", func() {
							Expect(elements).To(Equal([]int{6, 5, 4, 3, 2}))
						})

					})

				})
			})

			It("x", func() {

			})

		})

	})

	// Describe("Transaction", func() {
	// 	Context("When data value is created", func() {
	// 		BeforeEach(func() {
	// 			err = i.Transaction(func(m modifier.EntityWriter) error {
	// 				return m.CreateData(modifier.DBPath{"test"}, func(w io.Writer) error {
	// 					_, e := w.Write([]byte("test"))
	// 					return e
	// 				})
	// 			})
	// 		})
	// 		It("Should not return error", func() {
	// 			Expect(err).ToNot(HaveOccurred())
	// 		})
	//
	// 		Context("When I add a listener for that value", func() {
	// 			var data []byte
	// 			BeforeEach(func() {
	// 				i.AddListenerFunc(modifier.DBPath{"test"}, func(r modifier.EntityReader) {
	// 					reader, err := r.Data()
	// 					Expect(err).ToNot(HaveOccurred())
	// 					data, err = ioutil.ReadAll(reader)
	// 					Expect(err).ToNot(HaveOccurred())
	// 				})
	// 			})
	// 			It("Should call the listener with the value", func() {
	// 				Expect(string(data)).To(Equal("test"))
	// 			})
	// 			Context("When I change the value", func() {
	// 				BeforeEach(func() {
	// 					err = i.Transaction(func(m modifier.EntityWriter) error {
	// 						return m.CreateData(modifier.DBPath{"test"}, func(w io.Writer) error {
	// 							_, e := w.Write([]byte("test123"))
	// 							return e
	// 						})
	// 					})
	// 					Expect(err).ToNot(HaveOccurred())
	// 				})
	// 				It("Should call the listner with the new value", func() {
	// 					Expect(string(data)).To(Equal("test123"))
	// 				})
	//
	// 			})
	//
	// 		})
	// 	})
	// })
	//
	// Describe("AddListenerFunc", func() {
	//
	// 	var called bool
	// 	JustBeforeEach(func() {
	// 		i.AddListenerFunc(modifier.DBPath{"test"}, func(r modifier.EntityReader) {
	// 			called = true
	// 		})
	// 	})
	// 	Context("When the value does not exist", func() {
	// 		It("Should not call the listener", func() {
	// 			Expect(called).To(BeFalse())
	// 		})
	// 	})
	// 	Context("When the value exists", func() {
	// 		BeforeEach(func() {
	// 			err := i.Transaction(func(w modifier.EntityWriter) error {
	// 				return w.CreateHash(modifier.DBPath{"test"})
	// 			})
	// 			Expect(err).ToNot(HaveOccurred())
	// 		})
	// 		It("Should call the listener", func() {
	// 			Expect(called).To(BeTrue())
	// 		})
	// 	})
	// })
	//
	// Describe("RemoveListener", func() {
	// 	Context("When there is a listener registered", func() {
	// 		var called bool
	// 		var listener func(r modifier.EntityReader)
	// 		BeforeEach(func() {
	// 			err := i.Transaction(func(w modifier.EntityWriter) error {
	// 				return w.CreateHash(modifier.DBPath{"test"})
	// 			})
	// 			Expect(err).ToNot(HaveOccurred())
	// 			listener = func(r modifier.EntityReader) {
	// 				called = true
	// 			}
	// 			i.AddListenerFunc(modifier.DBPath{"test"}, listener)
	// 		})
	//
	// 		Context("When I remove the listener", func() {
	// 			BeforeEach(func() {
	// 				i.RemoveListenerFunc(modifier.DBPath{"test"}, listener)
	// 				called = false
	// 			})
	// 			Context("When I change the value", func() {
	// 				BeforeEach(func() {
	// 					err := i.Transaction(func(w modifier.EntityWriter) error {
	// 						return w.CreateHash(modifier.DBPath{"test", "test2"})
	// 					})
	// 					Expect(err).ToNot(HaveOccurred())
	// 				})
	// 				It("Should not call the listener", func() {
	// 					Expect(called).ToNot(BeTrue())
	// 				})
	// 			})
	// 		})
	//
	// 	})
	// })
	//
	// Describe("ReadTransaction", func() {
	// 	Context("When the database is empty", func() {
	// 		Context("When I get the size of the root", func() {
	// 			var s uint64
	// 			BeforeEach(func() {
	// 				i.ReadTransaction(func(r modifier.EntityReader) error {
	// 					s = r.Size()
	// 					return nil
	// 				})
	// 			})
	// 			It("Should return 0", func() {
	// 				Expect(s).To(Equal(uint64(0)))
	// 			})
	// 		})
	// 	})
	// 	Context("When the database has one value", func() {
	// 		BeforeEach(func() {
	// 			Expect(i.Transaction(func(w modifier.EntityWriter) error {
	// 				return w.CreateHash(modifier.DBPath{"test"})
	// 			}))
	// 		})
	// 		Context("When I get the size of the root", func() {
	// 			var s uint64
	// 			BeforeEach(func() {
	// 				i.ReadTransaction(func(r modifier.EntityReader) error {
	// 					s = r.Size()
	// 					return nil
	// 				})
	// 			})
	// 			It("Should return 1", func() {
	// 				Expect(s).To(Equal(uint64(1)))
	// 			})
	// 		})
	// 	})
	// })

})
