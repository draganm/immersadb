package dbpath

import (
	"net/url"
	"strings"

	"github.com/pkg/errors"
)

const Separator = "/"

func Split(path string) ([]string, error) {

	parts := strings.Split(path, Separator)

	res := []string{}

	for i, p := range parts {
		up, err := UnescapePart(p)
		if err != nil {
			return nil, errors.Wrapf(err, "while unescaping part at position %d: %q", i, p)
		}
		if up != "" {
			res = append(res, up)
		}
	}

	return res, nil
}

func Join(parts ...string) string {
	escaped := make([]string, len(parts))
	for i, p := range parts {
		escaped[i] = EscapePart(p)
	}
	return strings.Join(escaped, Separator)
}

func EscapePart(part string) string {
	return url.PathEscape(part)
}

func UnescapePart(part string) (string, error) {
	return url.PathUnescape(part)
}
