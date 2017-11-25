package immersadb_test

import (
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
						_, e := m.PrependMap(nil)
						return e
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
						_, e := m.PrependArray(nil)
						return e
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
						_, e := m.PrependArray(nil)
						return e
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
						_, e := m.PrependMap(nil)
						return e
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
})
