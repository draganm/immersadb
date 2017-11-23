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
