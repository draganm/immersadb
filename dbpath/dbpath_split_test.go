package dbpath_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/draganm/immersadb/dbpath"
)

func TestSplit(t *testing.T) {
	cases := []struct {
		title          string
		path           string
		expectedResult []string
		expectedError  string
	}{
		{
			title:          "empty",
			path:           "",
			expectedResult: []string{},
			expectedError:  "",
		},
		{
			title:          "root",
			path:           "/",
			expectedResult: []string{},
			expectedError:  "",
		},

		{
			title:          "empty and word",
			path:           "/test",
			expectedResult: []string{"test"},
			expectedError:  "",
		},
		{
			title:          "word and empty",
			path:           "test/",
			expectedResult: []string{"test"},
			expectedError:  "",
		},
		{
			title:          "escaped",
			path:           "%20/",
			expectedResult: []string{" "},
			expectedError:  "",
		},
		{
			title:          "invalid",
			path:           "%%/",
			expectedResult: nil,
			expectedError:  "while unescaping part at position 0: \"%%\": invalid URL escape \"%%\"",
		},
	}

	for _, tc := range cases {
		t.Run(tc.title, func(t *testing.T) {
			parts, err := dbpath.Split(tc.path)
			if tc.expectedError != "" {
				require.EqualError(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tc.expectedResult, parts)
		})
	}
}
