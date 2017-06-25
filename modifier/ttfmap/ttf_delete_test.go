package ttfmap_test

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/draganm/immersadb/modifier/ttfmap"
	"github.com/draganm/immersadb/store"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("2-3-4 Map", func() {
	var s *store.MemoryStore
	var err error
	var rootAddress uint64
	BeforeEach(func() {
		s = store.NewMemoryStore(nil)
	})

	Context("When I have regression tree with 10 nodes", func() {
		BeforeEach(func() {

			root := ttfmap.NewInMemoryLazyRootNode(s, map[string]uint64{"15": 100000, "3": 100001})

			c1, err := root.AddChild(map[string]uint64{"12": 100002})
			Expect(err).ToNot(HaveOccurred())

			_, err = c1.AddChild(map[string]uint64{"1": 100003, "11": 100004})
			Expect(err).ToNot(HaveOccurred())

			_, err = c1.AddChild(map[string]uint64{"14": 100005})
			Expect(err).ToNot(HaveOccurred())

			c2, err := root.AddChild(map[string]uint64{"18": 100006})
			Expect(err).ToNot(HaveOccurred())

			_, err = c2.AddChild(map[string]uint64{"17": 100007})
			Expect(err).ToNot(HaveOccurred())

			_, err = c2.AddChild(map[string]uint64{"2": 100008})
			Expect(err).ToNot(HaveOccurred())

			c3, err := root.AddChild(map[string]uint64{"6": 100002})
			Expect(err).ToNot(HaveOccurred())

			_, err = c3.AddChild(map[string]uint64{"5": 100009})
			Expect(err).ToNot(HaveOccurred())

			_, err = c3.AddChild(map[string]uint64{"7": 100010, "8": 100011, "9": 100012})
			Expect(err).ToNot(HaveOccurred())

			rootAddress, err = root.Store()
			Expect(err).ToNot(HaveOccurred())

			err = ttfmap.Validate(s, rootAddress)
			Expect(err).ToNot(HaveOccurred())

		})

		Context("When I delete '15' from the tree", func() {
			BeforeEach(func() {
				rootAddress, err = ttfmap.Delete(s, rootAddress, "15")
			})
			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should not violate 2-3-4 tree invariants", func() {
				err = ttfmap.Validate(s, rootAddress)
				Expect(err).ToNot(HaveOccurred())
			})

			Context("When I lookup value of 15", func() {
				BeforeEach(func() {
					_, err = ttfmap.Lookup(s, rootAddress, "15")
				})
				It("Should not be found", func() {
					Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
				})
			})

		})
	})

	Context("When I have regression tree with 10 nodes", func() {
		BeforeEach(func() {

			root := ttfmap.NewInMemoryLazyRootNode(s, map[string]uint64{"13": 100000, "19": 100001})

			c1, err := root.AddChild(map[string]uint64{"10": 100002})
			Expect(err).ToNot(HaveOccurred())
			_, err = c1.AddChild(map[string]uint64{"1": 100003})
			Expect(err).ToNot(HaveOccurred())
			_, err = c1.AddChild(map[string]uint64{"12": 100004})
			Expect(err).ToNot(HaveOccurred())

			c2, err := root.AddChild(map[string]uint64{"16": 100005})
			Expect(err).ToNot(HaveOccurred())
			_, err = c2.AddChild(map[string]uint64{"14": 100006})
			Expect(err).ToNot(HaveOccurred())
			_, err = c2.AddChild(map[string]uint64{"17": 100007})
			Expect(err).ToNot(HaveOccurred())

			c3, err := root.AddChild(map[string]uint64{"4": 100008})
			Expect(err).ToNot(HaveOccurred())
			_, err = c3.AddChild(map[string]uint64{"2": 100009})
			Expect(err).ToNot(HaveOccurred())
			_, err = c3.AddChild(map[string]uint64{"5": 100010})
			Expect(err).ToNot(HaveOccurred())

			rootAddress, err = root.Store()
			Expect(err).ToNot(HaveOccurred())

			err = ttfmap.Validate(s, rootAddress)
			Expect(err).ToNot(HaveOccurred())

		})

		Context("When I delete '2' from the tree", func() {
			BeforeEach(func() {
				rootAddress, err = ttfmap.Delete(s, rootAddress, "2")
				// graph.DumpGraph(s, rootAddress)
			})
			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should not violate 2-3-4 tree invariants", func() {
				err = ttfmap.Validate(s, rootAddress)
				Expect(err).ToNot(HaveOccurred())
			})

			Context("When I lookup value of 2", func() {
				BeforeEach(func() {
					_, err = ttfmap.Lookup(s, rootAddress, "2")
				})
				It("Should not be found", func() {
					Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
				})
			})

		})
	})

	Context("When I have regression tree with 9 nodes", func() {
		BeforeEach(func() {
			root := ttfmap.NewInMemoryLazyRootNode(s, map[string]uint64{"3": 100000})
			lc, err := root.AddChild(map[string]uint64{"11": 100001, "13": 100002})
			Expect(err).ToNot(HaveOccurred())
			_, err = lc.AddChild(map[string]uint64{"10": 100004})
			Expect(err).ToNot(HaveOccurred())
			_, err = lc.AddChild(map[string]uint64{"12": 100005})
			Expect(err).ToNot(HaveOccurred())
			_, err = lc.AddChild(map[string]uint64{"15": 100007, "2": 100008})

			// 3
			rc, err := root.AddChild(map[string]uint64{"8": 100009})
			Expect(err).ToNot(HaveOccurred())
			_, err = rc.AddChild(map[string]uint64{"6": 100010})
			Expect(err).ToNot(HaveOccurred())
			_, err = rc.AddChild(map[string]uint64{"9": 100012})
			Expect(err).ToNot(HaveOccurred())

			rootAddress, err = root.Store()
			Expect(err).ToNot(HaveOccurred())

			err = ttfmap.Validate(s, rootAddress)
			Expect(err).ToNot(HaveOccurred())

		})

		Context("When I delete '3' from the tree", func() {
			BeforeEach(func() {
				rootAddress, err = ttfmap.Delete(s, rootAddress, "3")
			})
			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should not violate 2-3-4 tree invariants", func() {
				err = ttfmap.Validate(s, rootAddress)
				Expect(err).ToNot(HaveOccurred())
			})

			Context("When I lookup value of 3", func() {
				BeforeEach(func() {
					_, err = ttfmap.Lookup(s, rootAddress, "3")
				})
				It("Should not be found", func() {
					Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
				})
			})

		})
	})

	Context("When I have regression tree with 6 nodes", func() {
		BeforeEach(func() {
			root := ttfmap.NewInMemoryLazyRootNode(s, map[string]uint64{"4": 100000})
			lc, err := root.AddChild(map[string]uint64{"10": 100001, "13": 100002, "15": 100003})
			Expect(err).ToNot(HaveOccurred())
			_, err = lc.AddChild(map[string]uint64{"0": 100004, "1": 100005})
			Expect(err).ToNot(HaveOccurred())
			_, err = lc.AddChild(map[string]uint64{"11": 100005, "12": 100006})
			Expect(err).ToNot(HaveOccurred())
			_, err = lc.AddChild(map[string]uint64{"14": 100007})
			Expect(err).ToNot(HaveOccurred())
			_, err = lc.AddChild(map[string]uint64{"3": 100008})
			Expect(err).ToNot(HaveOccurred())

			// 14
			rc, err := root.AddChild(map[string]uint64{"7": 100009})
			Expect(err).ToNot(HaveOccurred())
			_, err = rc.AddChild(map[string]uint64{"5": 100010, "6": 100011})
			Expect(err).ToNot(HaveOccurred())
			_, err = rc.AddChild(map[string]uint64{"8": 100012, "9": 100013})
			Expect(err).ToNot(HaveOccurred())

			rootAddress, err = root.Store()
			Expect(err).ToNot(HaveOccurred())

			err = ttfmap.Validate(s, rootAddress)
			Expect(err).ToNot(HaveOccurred())
		})
		Context("When I delete '14' from the tree", func() {
			BeforeEach(func() {
				rootAddress, err = ttfmap.Delete(s, rootAddress, "14")
			})
			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should not violate 2-3-4 tree invariants", func() {
				err = ttfmap.Validate(s, rootAddress)
				Expect(err).ToNot(HaveOccurred())
			})

			Context("When I lookup value of 14", func() {
				BeforeEach(func() {
					_, err = ttfmap.Lookup(s, rootAddress, "14")
				})
				It("Should not be found", func() {
					Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
				})
			})

		})
	})

	Context("When I have regression tree with 6 nodes - 2", func() {
		BeforeEach(func() {
			root := ttfmap.NewInMemoryLazyRootNode(s, map[string]uint64{"13": 100000})
			lc, err := root.AddChild(map[string]uint64{"11": 100001, "15": 100002})
			Expect(err).ToNot(HaveOccurred())
			_, err = lc.AddChild(map[string]uint64{"10": 100004})
			Expect(err).ToNot(HaveOccurred())
			_, err = lc.AddChild(map[string]uint64{"12": 100005})
			Expect(err).ToNot(HaveOccurred())
			_, err = lc.AddChild(map[string]uint64{"2": 100007})
			Expect(err).ToNot(HaveOccurred())

			// 2
			_, err = root.AddChild(map[string]uint64{"6": 100009})
			Expect(err).ToNot(HaveOccurred())

			rootAddress, err = root.Store()
			Expect(err).ToNot(HaveOccurred())

		})

		It("Should violate 2-3-4 tree invariants", func() {
			err = ttfmap.Validate(s, rootAddress)
			Expect(err).To(HaveOccurred())
		})

	})

	Context("When I have regression tree with 8 nodes", func() {
		BeforeEach(func() {
			root := ttfmap.NewInMemoryLazyRootNode(s, map[string]uint64{"2": 100000})
			lc, err := root.AddChild(map[string]uint64{"11": 100001, "13": 100002})
			Expect(err).ToNot(HaveOccurred())
			_, err = lc.AddChild(map[string]uint64{"10": 100004})
			Expect(err).ToNot(HaveOccurred())
			_, err = lc.AddChild(map[string]uint64{"12": 100005})
			Expect(err).ToNot(HaveOccurred())
			_, err = lc.AddChild(map[string]uint64{"15": 100007})
			Expect(err).ToNot(HaveOccurred())

			// 14
			rc, err := root.AddChild(map[string]uint64{"8": 100009})
			Expect(err).ToNot(HaveOccurred())
			_, err = rc.AddChild(map[string]uint64{"6": 100011})
			Expect(err).ToNot(HaveOccurred())
			_, err = rc.AddChild(map[string]uint64{"9": 100013})
			Expect(err).ToNot(HaveOccurred())

			rootAddress, err = root.Store()
			Expect(err).ToNot(HaveOccurred())

			err = ttfmap.Validate(s, rootAddress)
			Expect(err).ToNot(HaveOccurred())
		})
		Context("When I delete '6' from the tree", func() {
			BeforeEach(func() {
				rootAddress, err = ttfmap.Delete(s, rootAddress, "6")
			})
			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should not violate 2-3-4 tree invariants", func() {
				err = ttfmap.Validate(s, rootAddress)
				Expect(err).ToNot(HaveOccurred())
			})

			Context("When I lookup value of 6", func() {
				BeforeEach(func() {
					_, err = ttfmap.Lookup(s, rootAddress, "6")
				})
				It("Should not be found", func() {
					Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
				})
			})

		})
	})

	Context("When I add 500 elements to the tree in a random order", func() {

		var elementCount = 500

		BeforeEach(func() {
			rootAddress, err = ttfmap.CreateEmpty(s)
			Expect(err).ToNot(HaveOccurred())

			order := rand.Perm(elementCount)

			for _, i := range order {
				rootAddress, err = ttfmap.Insert(s, rootAddress, fmt.Sprintf("%d", i), 1000000+uint64(i))
				Expect(err).ToNot(HaveOccurred())
				Expect(ttfmap.Validate(s, rootAddress)).To(Succeed())
			}
		})

		Context("When I iterate over all keys", func() {
			var keys []string
			BeforeEach(func() {
				keys = nil
				err = ttfmap.ForEach(s, rootAddress, func(key string, value uint64) error {
					keys = append(keys, key)
					return nil
				})
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should have 20 keys", func() {
				Expect(len(keys)).To(Equal(elementCount))
			})
			It("Should have sorted keys", func() {
				Expect(sort.IsSorted(sort.StringSlice(keys))).To(BeTrue())
			})

			Context("When I delete keys in random order", func() {
				It("Should remove the key and keep the order", func() {
					var i int

					toRemove := rand.Perm(elementCount)
					for _, i = range toRemove {
						rootAddress, err = ttfmap.Delete(s, rootAddress, fmt.Sprintf("%d", i))
						Expect(err).ToNot(HaveOccurred())

						err = ttfmap.Validate(s, rootAddress)

						Expect(err).ToNot(HaveOccurred())
					}
				})
			})
		})
	})

	Context("Given an existing example 2-3-4 tree", func() {
		BeforeEach(func() {
			rootAddress, err = ttfmap.CreateEmpty(s)
			Expect(err).ToNot(HaveOccurred())

			root := ttfmap.NewInMemoryLazyRootNode(s, map[string]uint64{"P": 100000})
			lc, err := root.AddChild(map[string]uint64{"C": 100001, "H": 100002})
			Expect(err).ToNot(HaveOccurred())
			_, err = lc.AddChild(map[string]uint64{"A": 100003, "B": 100004})
			Expect(err).ToNot(HaveOccurred())
			_, err = lc.AddChild(map[string]uint64{"E": 100005, "F": 100006})
			Expect(err).ToNot(HaveOccurred())
			_, err = lc.AddChild(map[string]uint64{"N": 100007})
			Expect(err).ToNot(HaveOccurred())

			rc, err := root.AddChild(map[string]uint64{"V": 100008})
			Expect(err).ToNot(HaveOccurred())

			_, err = rc.AddChild(map[string]uint64{"R": 100009, "S": 100010})
			Expect(err).ToNot(HaveOccurred())

			_, err = rc.AddChild(map[string]uint64{"X": 100011, "Y": 100012, "Z": 100013})
			Expect(err).ToNot(HaveOccurred())

			rootAddress, err = root.Store()
			Expect(err).ToNot(HaveOccurred())

		})

		Context("When I delete element A", func() {
			BeforeEach(func() {
				rootAddress, err = ttfmap.Delete(s, rootAddress, "A")
			})
			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
			Context("When I lookup value of A", func() {
				BeforeEach(func() {
					_, err = ttfmap.Lookup(s, rootAddress, "A")
				})
				It("Should not be found", func() {
					Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
				})
			})
			It("Should contain all other keys", func() {
				keys := []string{}
				Expect(ttfmap.ForEach(s, rootAddress, func(key string, value uint64) error {
					keys = append(keys, key)
					return nil
				})).To(Succeed())
				Expect(keys).To(Equal([]string{"B", "C", "E", "F", "H", "N", "P", "R", "S", "V", "X", "Y", "Z"}))
			})
			Context("When I delete element B", func() {
				BeforeEach(func() {
					rootAddress, err = ttfmap.Delete(s, rootAddress, "B")
				})
				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})
				Context("When I lookup value of B", func() {
					BeforeEach(func() {
						_, err = ttfmap.Lookup(s, rootAddress, "B")
					})
					It("Should not be found", func() {
						Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
					})
				})
				It("Should contain all other keys", func() {
					keys := []string{}
					Expect(ttfmap.ForEach(s, rootAddress, func(key string, value uint64) error {
						keys = append(keys, key)
						return nil
					})).To(Succeed())
					Expect(keys).To(Equal([]string{"C", "E", "F", "H", "N", "P", "R", "S", "V", "X", "Y", "Z"}))
				})
			})

			// 3.1
			Context("When I delete element H", func() {
				BeforeEach(func() {
					rootAddress, err = ttfmap.Delete(s, rootAddress, "H")
				})
				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})
				Context("When I lookup value of H", func() {
					BeforeEach(func() {
						_, err = ttfmap.Lookup(s, rootAddress, "H")
					})
					It("Should not be found", func() {
						Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
					})
				})
				It("Should contain all other keys", func() {
					keys := []string{}
					Expect(ttfmap.ForEach(s, rootAddress, func(key string, value uint64) error {
						keys = append(keys, key)
						return nil
					})).To(Succeed())
					Expect(keys).To(Equal([]string{"B", "C", "E", "F", "N", "P", "R", "S", "V", "X", "Y", "Z"}))
				})
			})

			Context("When I delete element N", func() {
				BeforeEach(func() {
					rootAddress, err = ttfmap.Delete(s, rootAddress, "N")
				})
				It("Should not return error", func() {
					Expect(err).ToNot(HaveOccurred())
				})
				Context("When I lookup value of N", func() {
					BeforeEach(func() {
						_, err = ttfmap.Lookup(s, rootAddress, "N")
					})
					It("Should not be found", func() {
						Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
					})
				})
				It("Should contain all other keys", func() {
					keys := []string{}
					Expect(ttfmap.ForEach(s, rootAddress, func(key string, value uint64) error {
						keys = append(keys, key)
						return nil
					})).To(Succeed())
					Expect(keys).To(Equal([]string{"B", "C", "E", "F", "H", "P", "R", "S", "V", "X", "Y", "Z"}))
				})

				Context("When I delete element R", func() {
					BeforeEach(func() {
						rootAddress, err = ttfmap.Delete(s, rootAddress, "R")
					})
					It("Should not return error", func() {
						Expect(err).ToNot(HaveOccurred())
					})
					Context("When I lookup value of R", func() {
						BeforeEach(func() {
							_, err = ttfmap.Lookup(s, rootAddress, "R")
						})
						It("Should not be found", func() {
							Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
						})
					})
					It("Should contain all other keys", func() {
						keys := []string{}
						Expect(ttfmap.ForEach(s, rootAddress, func(key string, value uint64) error {
							keys = append(keys, key)
							return nil
						})).To(Succeed())
						Expect(keys).To(Equal([]string{"B", "C", "E", "F", "H", "P", "S", "V", "X", "Y", "Z"}))
					})
				})

				// 3.2 right
				Context("When I delete element B", func() {
					BeforeEach(func() {
						rootAddress, err = ttfmap.Delete(s, rootAddress, "B")
					})
					It("Should not return error", func() {
						Expect(err).ToNot(HaveOccurred())
					})
					Context("When I lookup value of B", func() {
						BeforeEach(func() {
							_, err = ttfmap.Lookup(s, rootAddress, "B")
						})
						It("Should not be found", func() {
							Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
						})
					})
					It("Should contain all other keys", func() {
						keys := []string{}
						Expect(ttfmap.ForEach(s, rootAddress, func(key string, value uint64) error {
							keys = append(keys, key)
							return nil
						})).To(Succeed())
						Expect(keys).To(Equal([]string{"C", "E", "F", "H", "P", "R", "S", "V", "X", "Y", "Z"}))
					})
				})

				// 3.2: left
				Context("When I delete element H", func() {
					BeforeEach(func() {
						rootAddress, err = ttfmap.Delete(s, rootAddress, "H")
					})
					It("Should not return error", func() {
						Expect(err).ToNot(HaveOccurred())
					})
					Context("When I lookup value of H", func() {
						BeforeEach(func() {
							_, err = ttfmap.Lookup(s, rootAddress, "H")
						})
						It("Should not be found", func() {
							Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
						})
					})
					It("Should contain all other keys", func() {
						keys := []string{}
						Expect(ttfmap.ForEach(s, rootAddress, func(key string, value uint64) error {
							keys = append(keys, key)
							return nil
						})).To(Succeed())
						Expect(keys).To(Equal([]string{"B", "C", "E", "F", "P", "R", "S", "V", "X", "Y", "Z"}))
					})

					Context("When I delete element H", func() {
						BeforeEach(func() {

							rootAddress, err = ttfmap.Delete(s, rootAddress, "H")
						})
						It("Should not return error", func() {
							Expect(err).ToNot(HaveOccurred())
						})
						Context("When I lookup value of H", func() {
							BeforeEach(func() {
								_, err = ttfmap.Lookup(s, rootAddress, "H")
							})
							It("Should not be found", func() {
								Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
							})
						})
						It("Should contain all other keys", func() {
							keys := []string{}
							Expect(ttfmap.ForEach(s, rootAddress, func(key string, value uint64) error {
								keys = append(keys, key)
								return nil
							})).To(Succeed())
							Expect(keys).To(Equal([]string{"B", "C", "E", "F", "P", "R", "S", "V", "X", "Y", "Z"}))
						})
					})

					// merge on the way down
					Context("When I delete element R", func() {
						BeforeEach(func() {
							rootAddress, err = ttfmap.Delete(s, rootAddress, "R")
						})
						It("Should not return error", func() {
							Expect(err).ToNot(HaveOccurred())
						})
						Context("When I lookup value of R", func() {
							BeforeEach(func() {

								_, err = ttfmap.Lookup(s, rootAddress, "R")
							})
							It("Should not be found", func() {
								Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
							})
						})
						It("Should contain all other keys", func() {
							keys := []string{}
							Expect(ttfmap.ForEach(s, rootAddress, func(key string, value uint64) error {
								keys = append(keys, key)
								return nil
							})).To(Succeed())
							Expect(keys).To(Equal([]string{"B", "C", "E", "F", "P", "S", "V", "X", "Y", "Z"}))
						})
						// 2.2: right
						Context("When I delete element C", func() {
							BeforeEach(func() {

								rootAddress, err = ttfmap.Delete(s, rootAddress, "C")
							})
							It("Should not return error", func() {
								Expect(err).ToNot(HaveOccurred())
							})
							Context("When I lookup value of C", func() {
								BeforeEach(func() {
									_, err = ttfmap.Lookup(s, rootAddress, "C")
								})
								It("Should not be found", func() {
									Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
								})
							})
							It("Should contain all other keys", func() {
								keys := []string{}
								Expect(ttfmap.ForEach(s, rootAddress, func(key string, value uint64) error {
									keys = append(keys, key)
									return nil
								})).To(Succeed())
								Expect(keys).To(Equal([]string{"B", "E", "F", "P", "S", "V", "X", "Y", "Z"}))
							})

							It("Should not violate 2-3-4 tree invariants", func() {
								err = ttfmap.Validate(s, rootAddress)
								Expect(err).ToNot(HaveOccurred())
							})

							// 2.3: two 2 children
							Context("When I delete element P", func() {
								BeforeEach(func() {
									rootAddress, err = ttfmap.Delete(s, rootAddress, "P")
								})
								It("Should not return error", func() {
									Expect(err).ToNot(HaveOccurred())
								})
								Context("When I lookup value of P", func() {
									BeforeEach(func() {
										_, err = ttfmap.Lookup(s, rootAddress, "P")
									})
									It("Should not be found", func() {
										Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
									})
								})
								It("Should contain all other keys", func() {
									keys := []string{}
									Expect(ttfmap.ForEach(s, rootAddress, func(key string, value uint64) error {
										keys = append(keys, key)
										return nil
									})).To(Succeed())
									Expect(keys).To(Equal([]string{"B", "E", "F", "S", "V", "X", "Y", "Z"}))
								})

								// 2.2
								Context("When I delete element E", func() {
									BeforeEach(func() {
										rootAddress, err = ttfmap.Delete(s, rootAddress, "E")
										// graph.DumpGraph(s, rootAddress)
									})
									It("Should not return error", func() {
										Expect(err).ToNot(HaveOccurred())
									})
									Context("When I lookup value of E", func() {
										BeforeEach(func() {
											_, err = ttfmap.Lookup(s, rootAddress, "E")
										})
										It("Should not be found", func() {
											Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
										})
									})
									It("Should contain all other keys", func() {
										keys := []string{}
										Expect(ttfmap.ForEach(s, rootAddress, func(key string, value uint64) error {
											keys = append(keys, key)
											return nil
										})).To(Succeed())
										Expect(keys).To(Equal([]string{"B", "F", "S", "V", "X", "Y", "Z"}))
									})

									// 2.3 two 2 children
									Context("When I delete element F", func() {
										BeforeEach(func() {
											rootAddress, err = ttfmap.Delete(s, rootAddress, "F")
										})
										It("Should not return error", func() {
											Expect(err).ToNot(HaveOccurred())
										})
										Context("When I lookup value of F", func() {
											BeforeEach(func() {
												_, err = ttfmap.Lookup(s, rootAddress, "F")
											})
											It("Should not be found", func() {
												Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
											})
										})
										It("Should contain all other keys", func() {
											keys := []string{}
											Expect(ttfmap.ForEach(s, rootAddress, func(key string, value uint64) error {
												keys = append(keys, key)
												return nil
											})).To(Succeed())
											Expect(keys).To(Equal([]string{"B", "S", "V", "X", "Y", "Z"}))
										})

										// 2.1
										Context("When I delete element V", func() {
											BeforeEach(func() {
												rootAddress, err = ttfmap.Delete(s, rootAddress, "V")
												// graph.DumpGraph(s, rootAddress)
											})
											It("Should not return error", func() {
												Expect(err).ToNot(HaveOccurred())
											})
											Context("When I lookup value of V", func() {
												BeforeEach(func() {
													_, err = ttfmap.Lookup(s, rootAddress, "V")
												})
												It("Should not be found", func() {
													Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
												})
											})
											It("Should contain all other keys", func() {
												keys := []string{}
												Expect(ttfmap.ForEach(s, rootAddress, func(key string, value uint64) error {
													keys = append(keys, key)
													return nil
												})).To(Succeed())
												Expect(keys).To(Equal([]string{"B", "S", "X", "Y", "Z"}))
											})

											// 3.1
											Context("When I delete element B", func() {
												BeforeEach(func() {
													rootAddress, err = ttfmap.Delete(s, rootAddress, "B")
													// graph.DumpGraph(s, rootAddress)
												})
												It("Should not return error", func() {
													Expect(err).ToNot(HaveOccurred())
												})
												Context("When I lookup value of B", func() {
													BeforeEach(func() {
														_, err = ttfmap.Lookup(s, rootAddress, "B")
													})
													It("Should not be found", func() {
														Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
													})
												})
												It("Should contain all other keys", func() {
													keys := []string{}
													Expect(ttfmap.ForEach(s, rootAddress, func(key string, value uint64) error {
														keys = append(keys, key)
														return nil
													})).To(Succeed())
													Expect(keys).To(Equal([]string{"S", "X", "Y", "Z"}))
												})

												// 3.1
												Context("When I delete element X", func() {
													BeforeEach(func() {
														rootAddress, err = ttfmap.Delete(s, rootAddress, "X")
														// graph.DumpGraph(s, rootAddress)
													})
													It("Should not return error", func() {
														Expect(err).ToNot(HaveOccurred())
													})
													Context("When I lookup value of X", func() {
														BeforeEach(func() {
															_, err = ttfmap.Lookup(s, rootAddress, "X")
														})
														It("Should not be found", func() {
															Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
														})
													})
													It("Should contain all other keys", func() {
														keys := []string{}
														Expect(ttfmap.ForEach(s, rootAddress, func(key string, value uint64) error {
															keys = append(keys, key)
															return nil
														})).To(Succeed())
														Expect(keys).To(Equal([]string{"S", "Y", "Z"}))
													})

													//
													Context("When I delete element Y", func() {
														BeforeEach(func() {
															rootAddress, err = ttfmap.Delete(s, rootAddress, "Y")
															// graph.DumpGraph(s, rootAddress)
														})
														It("Should not return error", func() {
															Expect(err).ToNot(HaveOccurred())
														})
														Context("When I lookup value of Y", func() {
															BeforeEach(func() {
																_, err = ttfmap.Lookup(s, rootAddress, "Y")
															})
															It("Should not be found", func() {
																Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
															})
														})
														It("Should contain all other keys", func() {
															keys := []string{}
															Expect(ttfmap.ForEach(s, rootAddress, func(key string, value uint64) error {
																keys = append(keys, key)
																return nil
															})).To(Succeed())
															Expect(keys).To(Equal([]string{"S", "Z"}))
														})

														Context("When I delete element S", func() {
															BeforeEach(func() {
																rootAddress, err = ttfmap.Delete(s, rootAddress, "S")
																// graph.DumpGraph(s, rootAddress)
															})
															It("Should not return error", func() {
																Expect(err).ToNot(HaveOccurred())
															})
															Context("When I lookup value of S", func() {
																BeforeEach(func() {
																	_, err = ttfmap.Lookup(s, rootAddress, "S")
																})
																It("Should not be found", func() {
																	Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
																})
															})
															It("Should contain all other keys", func() {
																keys := []string{}
																Expect(ttfmap.ForEach(s, rootAddress, func(key string, value uint64) error {
																	keys = append(keys, key)
																	return nil
																})).To(Succeed())
																Expect(keys).To(Equal([]string{"Z"}))
															})

															Context("When I delete element Z", func() {
																BeforeEach(func() {
																	rootAddress, err = ttfmap.Delete(s, rootAddress, "Z")
																	// graph.DumpGraph(s, rootAddress)
																})
																It("Should not return error", func() {
																	Expect(err).ToNot(HaveOccurred())
																})
																Context("When I lookup value of Z", func() {
																	BeforeEach(func() {
																		_, err = ttfmap.Lookup(s, rootAddress, "Z")
																	})
																	It("Should not be found", func() {
																		Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
																	})
																})
																It("Should contain all other keys", func() {
																	keys := []string{}
																	Expect(ttfmap.ForEach(s, rootAddress, func(key string, value uint64) error {
																		keys = append(keys, key)
																		return nil
																	})).To(Succeed())
																	Expect(keys).To(Equal([]string{}))
																})
															})

														})

													})

												})

											})

										})
									})

								})

							})

						})
					})

				})
			})
		})

		Context("When I delete element B", func() {
			BeforeEach(func() {
				rootAddress, err = ttfmap.Delete(s, rootAddress, "B")
			})
			It("Should not return error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
			Context("When I lookup value of B", func() {
				BeforeEach(func() {
					_, err = ttfmap.Lookup(s, rootAddress, "B")
				})
				It("Should not be found", func() {
					Expect(err).To(Equal(ttfmap.ErrKeyNotFound))
				})
			})
			It("Should contain all other keys", func() {
				keys := []string{}
				Expect(ttfmap.ForEach(s, rootAddress, func(key string, value uint64) error {
					keys = append(keys, key)
					return nil
				})).To(Succeed())
				Expect(keys).To(Equal([]string{"A", "C", "E", "F", "H", "N", "P", "R", "S", "V", "X", "Y", "Z"}))
			})
		})

	})

})
