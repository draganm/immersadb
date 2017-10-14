package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/draganm/immersadb"
	"github.com/draganm/immersadb/browser"
	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/gc"
	"github.com/draganm/immersadb/store"

	"gopkg.in/urfave/cli.v2"
)

func main() {

	app := cli.App{

		Commands: []*cli.Command{
			&cli.Command{
				Name: "browse",
				Action: func(c *cli.Context) error {
					fileName := c.Args().First()
					if len(fileName) == 0 {
						return errors.New("File name not provided")
					}
					imdb, err := immersadb.New(fileName, 8192)
					if err != nil {
						return err
					}
					server := browser.Browser(":8082", imdb)
					return server.ListenAndServe()
				},
			},
			&cli.Command{
				Name: "dump-segments",
				Action: func(c *cli.Context) error {
					dirName := c.Args().First()
					if len(dirName) == 0 {
						return errors.New("File name not provided")
					}
					ss, err := store.NewSegmentedStore(dirName, 10*1024*1024)
					if err != nil {
						return err
					}

					for addr := ss.FirstChunkAddress(); addr < ss.NextChunkAddress(); {
						c := ss.Chunk(addr)
						addr += 2 + uint64(len(c))

						fmt.Printf("%x: %d\n", addr, chunk.Type(c))
					}
					return nil
				},
			},
			&cli.Command{
				Name: "dump",
				Action: func(c *cli.Context) error {
					fileName := c.Args().First()
					if len(fileName) == 0 {
						return errors.New("File name not provided")
					}
					source, err := store.NewFileStore(fileName)
					if err != nil {
						return err
					}
					destination := store.NewMemoryStore(nil)
					err = gc.Copy(source, destination)
					if err != nil {
						return err
					}
					current := uint64(0)
					data := destination.Data()
					// log.Println(data)
					for current < uint64(len(data)) {
						fmt.Printf("# %d\n", current)
						fmt.Println(data[current : current+4])
						len := binary.BigEndian.Uint32(data[current:])

						fmt.Println(data[current+4 : current+6])
						refCount := int(binary.BigEndian.Uint16(data[current+6:]))
						fmt.Println(data[current+6 : current+8])
						for i := 0; i < refCount; i++ {
							beg := current + 8 + uint64(i)*8
							fmt.Printf(" -> %d\n", binary.BigEndian.Uint64(data[beg:beg+8]))
							fmt.Println(data[beg : beg+8])

						}

						body := data[int(current)+8+8*refCount : int(current)+4+int(len)]

						fmt.Printf("# %#v\n", string(body))

						fmt.Println(body)

						current += uint64(len) + 8
						fmt.Println(data[int(current)-4 : int(current)])

						fmt.Println()
					}
					return nil
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		panic(err)
	}

}
