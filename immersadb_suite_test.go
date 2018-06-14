package immersadb_test

import (
	"io"
	"io/ioutil"
	"os"

	"github.com/draganm/immersadb"
	"github.com/draganm/immersadb/dbpath"
	"github.com/draganm/immersadb/modifier"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestImmersadb(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Immersadb Suite")
}

var _ = Describe("ImmersaDB", func() {

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

	Describe("Transaction", func() {
		Context("When data value is created", func() {
			BeforeEach(func() {
				err = i.Transaction(func(w modifier.DBWriter) error {
					w.CreateData(dbpath.P("test"), func(w io.Writer) error {
						_, e := w.Write([]byte("test"))
						return e
					})
					return nil
				})
			})
			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			Context("When I add a listener for that value", func() {
				var data []byte
				BeforeEach(func() {
					i.AddListenerFunc(dbpath.P("test"), func(p dbpath.Path, r modifier.DBReader) {
						r.Read(p, func(r io.Reader) error {
							data, err = ioutil.ReadAll(r)
							return err
						})
					})
					Expect(err).ToNot(HaveOccurred())
				})

				It("Should call the listener with the value", func() {
					Expect(string(data)).To(Equal("test"))
				})

				Context("When I change the value", func() {
					BeforeEach(func() {
						err = i.Transaction(func(m modifier.DBWriter) error {
							m.CreateData(dbpath.P("test"), func(w io.Writer) error {
								_, e := w.Write([]byte("test123"))
								return e
							})
							return nil
						})
						Expect(err).ToNot(HaveOccurred())
					})
					It("Should call the listner with the new value", func() {
						Expect(string(data)).To(Equal("test123"))
					})

				})

			})
		})
	})
	//
	// Describe("AddListenerFunc", func() {
	//
	// 	var called bool
	// 	var reader modifier.EntityReader
	// 	JustBeforeEach(func() {
	// 		i.AddListenerFunc(dbpath.Path{"test"}, func(r modifier.EntityReader) {
	// 			called = true
	// 			reader = r
	// 		})
	// 	})
	// 	Context("When the value does not exist", func() {
	// 		It("Should call the listener with nil reader", func() {
	// 			Expect(called).To(BeTrue())
	// 			Expect(reader).To(BeNil())
	// 		})
	// 	})
	// 	Context("When the value exists", func() {
	// 		BeforeEach(func() {
	// 			err := i.Transaction(func(m modifier.MapWriter) error {
	// 				return m.CreateMap("test", func(m modifier.MapWriter) error {
	// 					return nil
	// 				})
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
	// 			err := i.Transaction(func(m modifier.MapWriter) error {
	// 				return m.CreateMap("test", func(m modifier.MapWriter) error {
	// 					return nil
	// 				})
	// 			})
	// 			Expect(err).ToNot(HaveOccurred())
	// 			listener = func(r modifier.EntityReader) {
	// 				called = true
	// 			}
	// 			i.AddListenerFunc(dbpath.Path{"test"}, listener)
	// 		})
	//
	// 		Context("When I remove the listener", func() {
	// 			BeforeEach(func() {
	// 				i.RemoveListenerFunc(dbpath.Path{"test"}, listener)
	// 				called = false
	// 			})
	// 			Context("When I change the value", func() {
	// 				BeforeEach(func() {
	// 					err := i.Transaction(func(m modifier.MapWriter) error {
	// 						return m.ModifyMap("test", func(m modifier.MapWriter) error {
	// 							return m.CreateArray("foo", nil)
	// 						})
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
	// 				i.ReadTransaction(func(m modifier.MapReader) error {
	// 					s = m.Size()
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
	// 			Expect(i.Transaction(func(m modifier.MapWriter) error {
	// 				return m.CreateMap("test", func(m modifier.MapWriter) error {
	// 					return nil
	// 				})
	// 			})).To(Succeed())
	// 		})
	// 		Context("When I get the size of the root", func() {
	// 			var s uint64
	// 			BeforeEach(func() {
	// 				i.ReadTransaction(func(m modifier.MapReader) error {
	// 					s = m.Size()
	// 					return nil
	// 				})
	// 			})
	// 			XIt("Should return 1", func() {
	// 				Expect(s).To(Equal(uint64(1)))
	// 			})
	// 		})
	// 	})
	// })

})
