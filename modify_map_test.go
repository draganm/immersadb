package immersadb_test

import (
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

	// Describe("Transaction", func() {
	// 	Context("When data value is created", func() {
	// 		BeforeEach(func() {
	// 			err = i.Transaction(func(m modifier.EntityWriter) error {
	// 				return m.CreateData(dbpath.Path{"test"}, func(w io.Writer) error {
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
	// 				i.AddListenerFunc(dbpath.Path{"test"}, func(r modifier.EntityReader) {
	// 					reader := r.Data()
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
	// 						return m.CreateData(dbpath.Path{"test"}, func(w io.Writer) error {
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

})
