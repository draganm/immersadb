package graph

import (
	"fmt"

	msgpack "gopkg.in/vmihailenco/msgpack.v2"

	"github.com/awalterschulze/gographviz"
	"github.com/draganm/immersadb/chunk"
	"github.com/draganm/immersadb/store"
)

func DumpGraph(s store.Store, addr uint64) {

	graphAst, _ := gographviz.Parse([]byte(`digraph G {}`))
	graph := gographviz.NewGraph()
	gographviz.Analyse(graphAst, graph)

	toDo := []uint64{addr}

	// graph := gographviz.NewGraph()

	for len(toDo) > 0 {
		current := toDo[0]
		toDo = toDo[1:]

		t, refs, data := chunk.Parts(s.Chunk(current))

		label := fmt.Sprintf("\"%#v\"", t)

		keys := []string{}

		if t == chunk.TTFMapNode {
			err := msgpack.Unmarshal(data, &keys)
			if err != nil {
				panic(err)
			}

			label = fmt.Sprintf("\"%s\"", keys)

		}

		if t == 0 {
			label = fmt.Sprintf("%d", current)
		}

		graph.AddNode("G", fmt.Sprintf("%d", current), map[string]string{"label": label})

		for i, r := range refs {
			label = "\"\""
			if t == chunk.TTFMapNode {
				if i < len(keys) {
					label = fmt.Sprintf("\"V_%d\"", i+1)
				} else {
					label = fmt.Sprintf("\"C_%d\"", i-len(keys)+1)
				}

			}
			graph.AddEdge(fmt.Sprintf("%d", current), fmt.Sprintf("%d", r), true, map[string]string{"label": label})
			toDo = append(toDo, r)
		}

	}

	fmt.Println(graph.String())

}
