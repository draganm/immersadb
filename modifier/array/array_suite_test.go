package array_test

import (
	"github.com/draganm/immersadb/modifier/array"
	"github.com/draganm/immersadb/store"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestArray(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Array Suite")
}

var _ = Describe("Array", func() {
	var s *store.MemoryStore
	var rootAddress uint64
	var err error
	BeforeEach(func() {
		s = store.NewMemoryStore(nil)
		rootAddress = 0
	})

	Describe("CreateEmpty()", func() {
		BeforeEach(func() {
			rootAddress, err = array.CreateEmpty(s)
		})
		It("Should not return error", func() {
			Expect(err).ToNot(HaveOccurred())
		})
		It("Should create empty array chunk", func() {
			Expect(s.Data()).To(Equal([]byte{
				0, 4,
				30, 0,
				0, 0,
			}))
		})
	})

	Describe("DeleteLast()", func() {
		JustBeforeEach(func() {
			rootAddress, err = array.DeleteLast(s, rootAddress)
		})

		BeforeEach(func() {
			rootAddress, err = array.CreateEmpty(s)
			Expect(err).ToNot(HaveOccurred())
		})
		Context("When deleting last from an empty array", func() {
			It("Should return error", func() {
				Expect(err).To(Equal(array.ErrDeletingFromEmpty))
			})
		})
		Context("When deleting from an array with 1 element", func() {
			BeforeEach(func() {
				rootAddress, err = array.Prepend(s, rootAddress, 100000000)
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should not return an error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should delete the last value from the array", func() {
				s := array.Size(s, rootAddress)
				Expect(s).To(Equal(uint64(0)))
			})
		})
		Context("When deleting from an array with 2 elements", func() {
			BeforeEach(func() {
				rootAddress, err = array.Prepend(s, rootAddress, 100000000)
				Expect(err).ToNot(HaveOccurred())
				rootAddress, err = array.Prepend(s, rootAddress, 100000001)
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should not return an error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should delete the last value from the array", func() {
				s := array.Size(s, rootAddress)
				Expect(s).To(Equal(uint64(1)))
			})
		})
	})

	Describe("Prepend()", func() {
		Context("When prepending to an empty array", func() {
			BeforeEach(func() {
				rootAddress, err = array.CreateEmpty(s)
				Expect(err).ToNot(HaveOccurred())
			})
			Context("When I prepend one element", func() {
				BeforeEach(func() {
					rootAddress, err = array.Prepend(s, rootAddress, 100000)
				})
				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})
				It("Should Create a level 0 array with one element", func() {
					Expect(s.Data()[rootAddress:]).To(Equal([]byte{
						0, 12,
						30,
						1,
						0, 0, 0, 0, 0, 1, 134, 160,
						0, 1,
					}))
				})
			})

			Context("When I prepend four elements", func() {
				BeforeEach(func() {
					rootAddress, err = array.Prepend(s, rootAddress, 100000)
					Expect(err).ToNot(HaveOccurred())
					rootAddress, err = array.Prepend(s, rootAddress, 100001)
					Expect(err).ToNot(HaveOccurred())
					rootAddress, err = array.Prepend(s, rootAddress, 100002)
					Expect(err).ToNot(HaveOccurred())
					rootAddress, err = array.Prepend(s, rootAddress, 100003)
				})
				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})
				It("Should Create a level 0 array with four elements", func() {
					Expect(s.Data()[rootAddress:]).To(Equal([]byte{
						0, 36,
						30,
						4,
						0, 0, 0, 0, 0, 1, 134, 163,
						0, 0, 0, 0, 0, 1, 134, 162,
						0, 0, 0, 0, 0, 1, 134, 161,
						0, 0, 0, 0, 0, 1, 134, 160,
						0, 4,
					}))
				})
				Context("When I prepend 5th element", func() {
					var oldRoot uint64
					BeforeEach(func() {
						oldRoot = rootAddress
						rootAddress, err = array.Prepend(s, rootAddress, 100004)
					})
					It("Should not return error", func() {
						Expect(err).ToNot(HaveOccurred())
					})
					It("Should Create a level 1 and two level 0 arrays with four and one element(s)", func() {
						Expect(s.Data()[oldRoot:]).To(Equal([]byte{
							0, 36,
							30,
							4,
							0, 0, 0, 0, 0, 1, 134, 163,
							0, 0, 0, 0, 0, 1, 134, 162,
							0, 0, 0, 0, 0, 1, 134, 161,
							0, 0, 0, 0, 0, 1, 134, 160,
							0, 4,

							0, 12,
							30,
							1,
							0, 0, 0, 0, 0, 1, 134, 164,
							0, 1,

							0, 20,
							30,
							2,
							0, 0, 0, 0, 0, 0, 0, 110,
							0, 0, 0, 0, 0, 0, 0, 72,
							1, 5,
						}))
					})

					Context("When I prepend 6th element", func() {
						BeforeEach(func() {
							oldRoot = rootAddress
							rootAddress, err = array.Prepend(s, rootAddress, 100005)
						})
						It("Should not return error", func() {
							Expect(err).ToNot(HaveOccurred())
						})
						It("Should Create a level 1 and two level 0 arrays with four and one element(s)", func() {
							Expect(s.Data()[oldRoot:]).To(Equal([]byte{
								0, 20,
								30,
								2,
								0, 0, 0, 0, 0, 0, 0, 110,
								0, 0, 0, 0, 0, 0, 0, 72,
								1, 5,

								0, 20,
								30,
								2,
								0, 0, 0, 0, 0, 1, 134, 165,
								0, 0, 0, 0, 0, 1, 134, 164,
								0, 2,

								0, 20,
								30,
								2,
								0, 0, 0, 0, 0, 0, 0, 146,
								0, 0, 0, 0, 0, 0, 0, 72,
								1, 6,
							}))
						})
						Context("When I prepend 6,7,8,9,10th element", func() {
							BeforeEach(func() {
								rootAddress, err = array.Prepend(s, rootAddress, 100006)
								Expect(err).ToNot(HaveOccurred())
								rootAddress, err = array.Prepend(s, rootAddress, 100007)
								Expect(err).ToNot(HaveOccurred())
								rootAddress, err = array.Prepend(s, rootAddress, 100008)
								Expect(err).ToNot(HaveOccurred())
								oldRoot = rootAddress
								rootAddress, err = array.Prepend(s, rootAddress, 100009)
							})
							It("Should not return error", func() {
								Expect(err).ToNot(HaveOccurred())
							})
							It("Should Create a level 1 and two level 0 arrays with four and one element(s)", func() {
								Expect(s.Data()[oldRoot:]).To(Equal([]byte{
									0, 28,
									30,
									3,
									0, 0, 0, 0, 0, 0, 1, 46,
									0, 0, 0, 0, 0, 0, 0, 242,
									0, 0, 0, 0, 0, 0, 0, 72,
									1, 9,

									0, 20,
									30,
									2,
									0, 0, 0, 0, 0, 1, 134, 169,
									0, 0, 0, 0, 0, 1, 134, 168,
									0, 2,

									0, 28,
									30,
									3,
									0, 0, 0, 0, 0, 0, 1, 90,
									0, 0, 0, 0, 0, 0, 0, 242,
									0, 0, 0, 0, 0, 0, 0, 72,
									1, 10,
								}))
							})

							Context("When I prepend up to 17th element", func() {
								BeforeEach(func() {
									for i := 10; i < 16; i++ {
										rootAddress, err = array.Prepend(s, rootAddress, 100000+uint64(i))
										Expect(err).ToNot(HaveOccurred())
									}
									oldRoot = rootAddress
									rootAddress, err = array.Prepend(s, rootAddress, 100016)
								})
								It("Should not return error", func() {
									Expect(err).ToNot(HaveOccurred())
								})
								It("Should Create a level 1 and two level 0 arrays with four and one element(s)", func() {
									Expect(s.Data()[oldRoot:]).To(Equal([]byte{
										0, 36,
										30,
										4,
										0, 0, 0, 0, 0, 0, 2, 194,
										0, 0, 0, 0, 0, 0, 1, 202,
										0, 0, 0, 0, 0, 0, 0, 242,
										0, 0, 0, 0, 0, 0, 0, 72,
										1, 16,

										0, 12,
										30,
										1,
										0, 0, 0, 0, 0, 1, 134, 176,
										0, 1,

										0, 12,
										30,
										1,
										0, 0, 0, 0, 0, 0, 3, 14,
										1, 1,

										0, 20,
										30, 2,
										0, 0, 0, 0, 0, 0, 3, 28,
										0, 0, 0, 0, 0, 0, 2, 232,
										2, 17,
									}))
								})
							})
						})
					})

				})

			})

		})
	})
})
