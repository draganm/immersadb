package browser

import (
	"errors"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/draganm/immersadb"
	"github.com/draganm/immersadb/dbpath"
	"github.com/draganm/immersadb/modifier"
)

var templates = template.Must(template.New("hash").Parse(`
{{define "Head"}}<body><p>NAV: {{range .}}/<a href="/{{.Path}}">{{.Name}}</a>{{end}}<p>{{end}}
{{define "DirEntry"}}<br><a href="/{{.Path}}">{{.Name}}</a>{{end}}
{{define "Tail"}}</body>{{end}}
`))

type Breadcrumbs []DirEntry

type DirEntry struct {
	Name string
	Path string
}

func breadcrumbsForPath(p dbpath.Path) Breadcrumbs {
	b := Breadcrumbs{}
	for i, e := range p {
		b = append(b, DirEntry{
			Name: pathElementToString(e),
			Path: pathToString(p[:i]),
		})
	}
	return b
}

func pathElementToString(p interface{}) string {
	switch p.(type) {
	case string:
		return p.(string)
	case int:
		return strconv.Itoa(p.(int))
	default:
		return "???"
	}
}

func pathToString(pth dbpath.Path) string {
	parts := []string{}
	for _, p := range pth {
		parts = append(parts, pathElementToString(p))
	}
	return path.Join(parts...)
}

func Browser(addr string, db *immersadb.ImmersaDB) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Path

		err := db.ReadTransactionOld(func(r modifier.EntityReader) error {
			parts := strings.Split(name, "/")
			path := dbpath.Path{}
			for _, p := range parts {
				if p == "" {
					continue
				}
				sr := r.EntityReaderFor(path)
				switch sr.Type() {
				case modifier.Map:
					path = append(path, p)
				case modifier.Array:
					idx, err := strconv.ParseUint(p, 10, 64)
					if err != nil {
						return err
					}
					path = append(path, int(idx))
				default:
					return errors.New("Wrong path")
				}
			}

			er := r.EntityReaderFor(path)
			switch er.Type() {
			case modifier.Data:
				var r io.Reader
				r = er.Data()
				_, err := io.Copy(w, r)
				return err
			case modifier.Array:
				w.Header().Set("Content-Type", "text/html")

				err := templates.ExecuteTemplate(w, "Head", breadcrumbsForPath(path))
				if err != nil {
					return nil
				}

				err = er.ForEachArrayElement(func(index uint64, reader modifier.EntityReader) error {
					return templates.ExecuteTemplate(w, "DirEntry", DirEntry{Name: fmt.Sprintf("#%d", index), Path: pathToString(append(path, int(index)))})
				})
				if err != nil {
					return err
				}
				err = templates.ExecuteTemplate(w, "Tail", nil)
				if err != nil {
					return nil
				}

				return nil
			case modifier.Map:
				w.Header().Set("Content-Type", "text/html")

				err := templates.ExecuteTemplate(w, "Head", breadcrumbsForPath(path))
				if err != nil {
					return nil
				}

				err = er.ForEachMapEntry(func(key string, reader modifier.EntityReader) error {
					return templates.ExecuteTemplate(w, "DirEntry", DirEntry{Name: key, Path: pathToString(append(path, key))})
				})

				if err != nil {
					return err
				}
				err = templates.ExecuteTemplate(w, "Tail", nil)
				if err != nil {
					return nil
				}

				return nil
			default:
				w.Write([]byte("UNKNOWN"))
				return nil
			}
		})
		if err != nil {
			http.Error(w, err.Error(), 500)
		}
	},
	)
	s := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return s
}
