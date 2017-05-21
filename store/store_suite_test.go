package store_test

import (
	"io/ioutil"
	"os"
	"path"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/draganm/immersadb/store"

	"testing"
)

func TestFileStore(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "FileStore Suite")
}

var _ = Describe("FileStore", func() {

	var dir string
	var dbFile string
	BeforeEach(func() {
		var err error
		dir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())
		dbFile = path.Join(dir, "db.data")
	})

	AfterEach(func() {
		if dir != "" {
			Expect(os.RemoveAll(dir)).To(Succeed())
		}
	})

	Describe("NewFileStore()", func() {
		Context("When file does not exist", func() {
			var s *store.FileStore
			var err error
			BeforeEach(func() {
				s, err = store.NewFileStore(dbFile)
			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should create a new file", func() {
				_, err := os.Stat(dbFile)
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should return a new FileStore", func() {
				Expect(s).ToNot(BeNil())
			})

		})

		Context("When the file is empty", func() {
			var s *store.FileStore
			var err error
			BeforeEach(func() {
				s, err = store.NewFileStore(dbFile)
				Expect(err).ToNot(HaveOccurred())
				Expect(s.Close()).To(Succeed())
				s, err = store.NewFileStore(dbFile)

			})

			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
			Context("When I append one byte chunk", func() {

				var err error
				var id uint64

				BeforeEach(func() {
					id, err = s.Append([]byte{42})
				})

				It("Should not return an error", func() {
					Expect(err).ToNot(HaveOccurred())
				})

				It("Should return address 0", func() {
					Expect(id).To(Equal(uint64(0)))
				})

				It("Should create proper layout of the data", func() {
					data, err := ioutil.ReadFile(dbFile)
					Expect(err).ToNot(HaveOccurred())
					Expect(data).To(Equal([]byte{
						0, 0, 0, 1,
						42,
						0, 0, 0, 1,
					}))
				})
			})

			Context("When there is one chunk in the file already", func() {
				var s *store.FileStore
				var err error
				BeforeEach(func() {
					s, err = store.NewFileStore(dbFile)
					Expect(err).ToNot(HaveOccurred())
					_, err = s.Append([]byte{42})
					Expect(err).ToNot(HaveOccurred())
					Expect(s.Close()).To(Succeed())
					s, err = store.NewFileStore(dbFile)

				})

				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})
				Context("When I append one byte chunk", func() {

					var err error
					var id uint64

					BeforeEach(func() {
						id, err = s.Append([]byte{43})
					})

					It("Should not return an error", func() {
						Expect(err).ToNot(HaveOccurred())
					})

					It("Should return address 9", func() {
						Expect(id).To(Equal(uint64(9)))
					})

					It("Should create proper layout of the data", func() {
						data, err := ioutil.ReadFile(dbFile)
						Expect(err).ToNot(HaveOccurred())
						Expect(data).To(Equal([]byte{
							0, 0, 0, 1,
							42,
							0, 0, 0, 1,

							0, 0, 0, 1,
							43,
							0, 0, 0, 1,
						}))
					})
				})
			})
		})
	})

	Describe(".LastChunkAddress()", func() {
		var s *store.FileStore
		BeforeEach(func() {
			var err error
			s, err = store.NewFileStore(dbFile)
			Expect(err).ToNot(HaveOccurred())
		})
		Context("When there are no chunks in the store", func() {
			It("Should return 0", func() {
				Expect(s.LastChunkAddress()).To(Equal(uint64(0)))
			})
		})
		Context("When there is one chunk in the store", func() {
			BeforeEach(func() {
				_, err := s.Append([]byte{1})
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should return 0", func() {
				Expect(s.LastChunkAddress()).To(Equal(uint64(0)))
			})
		})
		Context("When there are two chunks in the store", func() {
			BeforeEach(func() {
				_, err := s.Append([]byte{1})
				Expect(err).ToNot(HaveOccurred())
				_, err = s.Append([]byte{2})
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should return address of the second chang", func() {
				Expect(s.LastChunkAddress()).To(Equal(uint64(9)))
			})
		})
	})

	Describe(".LastChunk()", func() {
		var s *store.FileStore
		BeforeEach(func() {
			var err error
			s, err = store.NewFileStore(dbFile)
			Expect(err).ToNot(HaveOccurred())
		})
		Context("When there are no chunks in the store", func() {
			It("Should return nil", func() {
				Expect(s.LastChunk()).To(BeNil())
			})
		})
		Context("When there is one chunk in the store", func() {
			BeforeEach(func() {
				_, err := s.Append([]byte{1})
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should return content of that chunk", func() {
				Expect(s.LastChunk()).To(Equal([]byte{1}))
			})
		})
		Context("When there are two chunks in the store", func() {
			BeforeEach(func() {
				_, err := s.Append([]byte{1})
				Expect(err).ToNot(HaveOccurred())
				_, err = s.Append([]byte{2})
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should return content of the last chunk", func() {
				Expect(s.LastChunk()).To(Equal([]byte{2}))
			})
		})
	})

	Describe(".Close()", func() {
		var s *store.FileStore
		BeforeEach(func() {
			var err error
			s, err = store.NewFileStore(dbFile)
			Expect(err).ToNot(HaveOccurred())
		})
		It("Should not return error", func() {
			Expect(s.Close()).To(Succeed())
		})
	})

	Describe(".Chunk()", func() {
		var s *store.FileStore
		BeforeEach(func() {
			var err error
			s, err = store.NewFileStore(dbFile)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("When there is nothing in the store", func() {
			Context("When I request first chunk", func() {
				It("Should return nil", func() {
					Expect(s.Chunk(1)).To(BeNil())
				})
			})
			Context("When I request any other chunk", func() {
				It("Should return nil", func() {
					Expect(s.Chunk(1334)).To(BeNil())
				})
			})
		})
		Context("When there is only one chunk in the store", func() {
			BeforeEach(func() {
				_, err := s.Append([]byte{1, 2, 3})
				Expect(err).ToNot(HaveOccurred())
			})
			Context("When I request first chunk", func() {
				It("Should return the chunk", func() {
					Expect(s.Chunk(0)).To(Equal([]byte{1, 2, 3}))
				})
			})
			Context("When I request any other chunk", func() {
				It("Should return nil", func() {
					Expect(s.Chunk(15)).To(BeNil())
				})
			})

		})

		Context("When there are two chunks in the store", func() {
			BeforeEach(func() {
				_, err := s.Append([]byte{1, 2, 3})
				Expect(err).ToNot(HaveOccurred())
				_, err = s.Append([]byte{4, 5, 6})
				Expect(err).ToNot(HaveOccurred())
			})
			Context("When I request the first chunk", func() {
				It("Should return the chunk", func() {
					Expect(s.Chunk(0)).To(Equal([]byte{1, 2, 3}))
				})
			})
			Context("When I request the second chunk", func() {
				It("Should return the chunk", func() {
					Expect(s.Chunk(11)).To(Equal([]byte{4, 5, 6}))
				})
			})
			Context("When I request any other chunk", func() {
				It("Should return nil", func() {
					Expect(s.Chunk(22)).To(BeNil())
				})
			})

		})

		Context("When there are three chunks in the store", func() {
			BeforeEach(func() {
				_, err := s.Append([]byte{1, 2, 3})
				Expect(err).ToNot(HaveOccurred())
				_, err = s.Append([]byte{4, 5, 6})
				Expect(err).ToNot(HaveOccurred())
				_, err = s.Append([]byte{7, 8, 9})
				Expect(err).ToNot(HaveOccurred())
			})
			Context("When I request the first chunk", func() {
				It("Should return the chunk", func() {
					Expect(s.Chunk(0)).To(Equal([]byte{1, 2, 3}))
				})
			})
			Context("When I request the second chunk", func() {
				It("Should return the chunk", func() {
					Expect(s.Chunk(11)).To(Equal([]byte{4, 5, 6}))
				})
			})
			Context("When I request the third chunk", func() {
				It("Should return the chunk", func() {
					Expect(s.Chunk(22)).To(Equal([]byte{7, 8, 9}))
				})
			})

			Context("When I request any other chunk", func() {
				It("Should return nil", func() {
					Expect(s.Chunk(99)).To(BeNil())
				})
			})

		})

		Context("When there are four chunks in the store", func() {
			BeforeEach(func() {
				_, err := s.Append([]byte{1, 2, 3})
				Expect(err).ToNot(HaveOccurred())
				_, err = s.Append([]byte{4, 5, 6})
				Expect(err).ToNot(HaveOccurred())
				_, err = s.Append([]byte{7, 8, 9})
				Expect(err).ToNot(HaveOccurred())
				_, err = s.Append([]byte{10, 11, 12})
				Expect(err).ToNot(HaveOccurred())
			})
			Context("When I request the first chunk", func() {
				It("Should return the chunk", func() {
					Expect(s.Chunk(0)).To(Equal([]byte{1, 2, 3}))
				})
			})
			Context("When I request the second chunk", func() {
				It("Should return the chunk", func() {
					Expect(s.Chunk(11)).To(Equal([]byte{4, 5, 6}))
				})
			})
			Context("When I request the third chunk", func() {
				It("Should return the chunk", func() {
					Expect(s.Chunk(22)).To(Equal([]byte{7, 8, 9}))
				})
			})
			Context("When I request the fourth chunk", func() {
				It("Should return the chunk", func() {
					Expect(s.Chunk(33)).To(Equal([]byte{10, 11, 12}))
				})
			})

			Context("When I request any other chunk", func() {
				It("Should return nil", func() {
					Expect(s.Chunk(99)).To(BeNil())
				})
			})

		})

		Context("When there are four chunks in the store", func() {
			BeforeEach(func() {
				_, err := s.Append([]byte{1, 2, 3})
				Expect(err).ToNot(HaveOccurred())
				_, err = s.Append([]byte{4, 5, 6})
				Expect(err).ToNot(HaveOccurred())
				_, err = s.Append([]byte{7, 8, 9})
				Expect(err).ToNot(HaveOccurred())
				_, err = s.Append([]byte{10, 11, 12})
				Expect(err).ToNot(HaveOccurred())
				_, err = s.Append([]byte{13, 14, 15})
				Expect(err).ToNot(HaveOccurred())
			})
			Context("When I request the first chunk", func() {
				It("Should return the chunk", func() {
					Expect(s.Chunk(0)).To(Equal([]byte{1, 2, 3}))
				})
			})
			Context("When I request the second chunk", func() {
				It("Should return the chunk", func() {
					Expect(s.Chunk(11)).To(Equal([]byte{4, 5, 6}))
				})
			})
			Context("When I request the third chunk", func() {
				It("Should return the chunk", func() {
					Expect(s.Chunk(22)).To(Equal([]byte{7, 8, 9}))
				})
			})
			Context("When I request the fourth chunk", func() {
				It("Should return the chunk", func() {
					Expect(s.Chunk(33)).To(Equal([]byte{10, 11, 12}))
				})
			})
			Context("When I request the fifth chunk", func() {
				It("Should return the chunk", func() {
					Expect(s.Chunk(44)).To(Equal([]byte{13, 14, 15}))
				})
			})

			Context("When I request any other chunk", func() {
				It("Should return nil", func() {
					Expect(s.Chunk(99)).To(BeNil())
				})
			})

		})

	})

	Describe(".Append", func() {
		var s *store.FileStore
		BeforeEach(func() {
			var err error
			s, err = store.NewFileStore(dbFile)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("When there are no chunks in the store", func() {
			Context("When I append one byte chunk", func() {

				var err error
				var id uint64

				BeforeEach(func() {
					id, err = s.Append([]byte{42})
				})

				It("Should not return an error", func() {
					Expect(err).ToNot(HaveOccurred())
				})

				It("Should return address 0", func() {
					Expect(id).To(Equal(uint64(0)))
				})

				It("Should create proper layout of the data", func() {
					data, err := ioutil.ReadFile(dbFile)
					Expect(err).ToNot(HaveOccurred())
					Expect(data).To(Equal([]byte{
						0, 0, 0, 1,
						42,
						0, 0, 0, 1,
					}))
				})

				Context("When I append another one byte chunk", func() {
					BeforeEach(func() {
						id, err = s.Append([]byte{43})
					})

					It("Should not return an error", func() {
						Expect(err).ToNot(HaveOccurred())
					})

					It("Should return address 9", func() {
						Expect(id).To(Equal(uint64(9)))
					})
					It("Should create proper layout of the data", func() {
						data, err := ioutil.ReadFile(dbFile)
						Expect(err).ToNot(HaveOccurred())
						Expect(data).To(Equal([]byte{
							0, 0, 0, 1,
							42,
							0, 0, 0, 1,

							0, 0, 0, 1,
							43,
							0, 0, 0, 1,
						}))
					})
					Context("When I append third byte chunk", func() {
						BeforeEach(func() {
							id, err = s.Append([]byte{44})
						})

						It("Should not return an error", func() {
							Expect(err).ToNot(HaveOccurred())
						})

						It("Should return address 3", func() {
							Expect(id).To(Equal(uint64(18)))
						})
						It("Should create proper layout of the data", func() {
							data, err := ioutil.ReadFile(dbFile)
							Expect(err).ToNot(HaveOccurred())
							Expect(data).To(Equal([]byte{
								0, 0, 0, 1,
								42,
								0, 0, 0, 1,

								0, 0, 0, 1,
								43,
								0, 0, 0, 1,

								0, 0, 0, 1,
								44,
								0, 0, 0, 1,
							}))
						})

						Context("When I append fourth byte chunk", func() {
							BeforeEach(func() {
								id, err = s.Append([]byte{45})
							})

							It("Should not return an error", func() {
								Expect(err).ToNot(HaveOccurred())
							})

							It("Should return address 27", func() {
								Expect(id).To(Equal(uint64(27)))
							})
							It("Should create proper layout of the data", func() {
								data, err := ioutil.ReadFile(dbFile)
								Expect(err).ToNot(HaveOccurred())
								Expect(data).To(Equal([]byte{
									0, 0, 0, 1,
									42,
									0, 0, 0, 1,

									0, 0, 0, 1,
									43,
									0, 0, 0, 1,

									0, 0, 0, 1,
									44,
									0, 0, 0, 1,

									0, 0, 0, 1,
									45,
									0, 0, 0, 1,
								}))
							})
						})
					})

				})
			})
		})

	})

})
