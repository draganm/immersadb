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
	"gopkg.in/vmihailenco/msgpack.v2"
)

var _ = Describe("ImmersaDB: array round robin", func() {

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

	Context("When I create an array", func() {
		BeforeEach(func() {
			err := i.Transaction(func(m modifier.MapWriter) error {
				return m.CreateArray("ar", nil)

			})
			Expect(err).ToNot(HaveOccurred())
		})
		Context("When I prepend 5 values to the array in 5 transactions", func() {
			BeforeEach(func() {
				for j := 0; j < 5; j++ {
					err := i.Transaction(func(m modifier.MapWriter) error {
						return m.ModifyArray("ar", func(m modifier.ArrayWriter) error {
							return m.PrependData(func(w io.Writer) error {
								return msgpack.NewEncoder(w).Encode(j)
							})
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
						ser := er.EntityReaderFor(dbpath.Path{"ar"})
						return ser.ForEachArrayElement(func(index uint64, reader modifier.EntityReader) error {
							var el int
							r := reader.Data()
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
						err := i.Transaction(func(m modifier.MapWriter) error {

							err := m.ModifyArray("ar", func(m modifier.ArrayWriter) error {
								return m.PrependData(func(w io.Writer) error {
									return msgpack.NewEncoder(w).Encode(5)
								})
							})

							if err != nil {
								return err
							}

							err = m.ModifyArray("ar", func(m modifier.ArrayWriter) error {
								return m.DeleteLast()
							})

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
							ser := er.EntityReaderFor(dbpath.Path{"ar"})
							size = ser.Size()
							return nil
						})
						Expect(err).ToNot(HaveOccurred())
						Expect(size).To(Equal(uint64(5)))
					})

					Context("When I rotate one more element", func() {
						BeforeEach(func() {
							err := i.Transaction(func(m modifier.MapWriter) error {

								err := m.ModifyArray("ar", func(m modifier.ArrayWriter) error {
									return m.PrependData(func(w io.Writer) error {
										return msgpack.NewEncoder(w).Encode(6)
									})
								})

								if err != nil {
									return err

								}

								err = m.ModifyArray("ar", func(m modifier.ArrayWriter) error {
									return m.DeleteLast()
								})

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

})
