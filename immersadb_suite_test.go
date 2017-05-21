package immersadb_test

import (
	"io"
	"io/ioutil"
	"os"
	"path"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/draganm/immersadb"
	"github.com/draganm/immersadb/modifier"

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

	Describe("Transaction", func() {
		Context("When data value is created", func() {
			BeforeEach(func() {
				err = i.Transaction(func(m modifier.EntityWriter) error {
					return m.CreateData(modifier.DBPath{"test"}, func(w io.Writer) error {
						_, e := w.Write([]byte("test"))
						return e
					})
				})
			})
			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			Context("When I add a listener for that value", func() {
				var data []byte
				BeforeEach(func() {
					i.AddListenerFunc(modifier.DBPath{"test"}, func(r modifier.EntityReader) {
						reader, err := r.Data()
						Expect(err).ToNot(HaveOccurred())
						data, err = ioutil.ReadAll(reader)
						Expect(err).ToNot(HaveOccurred())
					})
				})
				It("Should call the listener with the value", func() {
					Expect(string(data)).To(Equal("test"))
				})
				Context("When I change the value", func() {
					BeforeEach(func() {
						err = i.Transaction(func(m modifier.EntityWriter) error {
							return m.CreateData(modifier.DBPath{"test"}, func(w io.Writer) error {
								_, e := w.Write([]byte("test123"))
								return e
							})
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

	Describe("AddListenerFunc", func() {

		var called bool
		JustBeforeEach(func() {
			i.AddListenerFunc(modifier.DBPath{"test"}, func(r modifier.EntityReader) {
				called = true
			})
		})
		Context("When the value does not exist", func() {
			It("Should not call the listener", func() {
				Expect(called).To(BeFalse())
			})
		})
		Context("When the value exists", func() {
			BeforeEach(func() {
				err := i.Transaction(func(w modifier.EntityWriter) error {
					return w.CreateHash(modifier.DBPath{"test"})
				})
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should call the listener", func() {
				Expect(called).To(BeTrue())
			})
		})
	})

	Describe("RemoveListener", func() {
		Context("When there is a listener registered", func() {
			var called bool
			var listener func(r modifier.EntityReader)
			BeforeEach(func() {
				err := i.Transaction(func(w modifier.EntityWriter) error {
					return w.CreateHash(modifier.DBPath{"test"})
				})
				Expect(err).ToNot(HaveOccurred())
				listener = func(r modifier.EntityReader) {
					called = true
				}
				i.AddListenerFunc(modifier.DBPath{"test"}, listener)
			})

			Context("When I remove the listener", func() {
				BeforeEach(func() {
					i.RemoveListenerFunc(modifier.DBPath{"test"}, listener)
					called = false
				})
				Context("When I change the value", func() {
					BeforeEach(func() {
						err := i.Transaction(func(w modifier.EntityWriter) error {
							return w.CreateHash(modifier.DBPath{"test", "test2"})
						})
						Expect(err).ToNot(HaveOccurred())
					})
					It("Should not call the listener", func() {
						Expect(called).ToNot(BeTrue())
					})
				})
			})

		})
	})

	Describe("ReadTransaction", func() {
		Context("When the database is empty", func() {
			Context("When I get the size of the root", func() {
				var s uint64
				BeforeEach(func() {
					i.ReadTransaction(func(r modifier.EntityReader) error {
						s = r.Size()
						return nil
					})
				})
				It("Should return 0", func() {
					Expect(s).To(Equal(uint64(0)))
				})
			})
		})
		Context("When the database has one value", func() {
			BeforeEach(func() {
				Expect(i.Transaction(func(w modifier.EntityWriter) error {
					return w.CreateHash(modifier.DBPath{"test"})
				}))
			})
			Context("When I get the size of the root", func() {
				var s uint64
				BeforeEach(func() {
					i.ReadTransaction(func(r modifier.EntityReader) error {
						s = r.Size()
						return nil
					})
				})
				It("Should return 1", func() {
					Expect(s).To(Equal(uint64(1)))
				})
			})
		})
	})

})
