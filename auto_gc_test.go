package immersadb_test

import (
	"io/ioutil"
	"os"

	"github.com/draganm/immersadb"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Auto GC", func() {

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

	// Context("When I write lots of data to the db", func() {
	// 	BeforeEach(func() {
	// 		for j := 0; j < 2000; j++ {
	// 			Expect(i.Transaction(func(m modifier.MapWriter) error {
	// 				return m.CreateMap(fmt.Sprintf("%x", j), nil)
	// 			})).To(Succeed())
	// 		}
	// 	})
	//
	// 	Context("When I re-open the database", func() {
	// 		BeforeEach(func() {
	// 			i.Close()
	// 			id, err := immersadb.New(dir)
	// 			Expect(err).ToNot(HaveOccurred())
	// 			i = id
	// 		})
	//
	// 		It("Should keep it's state", func() {
	// 			Expect(i.ReadTransaction(func(m modifier.MapReader) error {
	// 				Expect(m.Size()).To(Equal(uint64(2000)))
	// 				return nil
	// 			}))
	// 		})
	//
	// 	})
	//
	// })
})
