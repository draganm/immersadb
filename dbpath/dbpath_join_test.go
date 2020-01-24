package dbpath_test

import (
	"testing"

	"github.com/draganm/immersadb/dbpath"
	"github.com/stretchr/testify/require"
)

func TestJoin(t *testing.T) {
	cases := []struct {
		title          string
		parts          []string
		expectedResult string
	}{
		{
			title:          "empty",
			parts:          nil,
			expectedResult: "",
		},
		{
			title:          "one element",
			parts:          []string{"test"},
			expectedResult: "test",
		},
		{
			title:          "two elements",
			parts:          []string{"foo", "bar"},
			expectedResult: "foo/bar",
		},
		{
			title:          "two elements with space",
			parts:          []string{"foo", "bar "},
			expectedResult: "foo/bar%20",
		},
		{
			title:          "two elements with slash",
			parts:          []string{"foo", "bar/"},
			expectedResult: "foo/bar%2F",
		},
	}

	for _, tc := range cases {
		t.Run(tc.title, func(t *testing.T) {
			require.Equal(t, tc.expectedResult, dbpath.Join(tc.parts...))
		})
	}
}
