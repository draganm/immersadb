package trie

func commonPrefix(p1, p2 []byte) ([]byte, []byte, []byte) {

	maxIndex := len(p1)
	if len(p2) < maxIndex {
		maxIndex = len(p2)
	}

	for i := 0; i < maxIndex; i++ {
		if p1[i] != p2[i] {
			return p1[:i], p1[i:], p2[i:]
		}
	}

	return p1[:maxIndex], p1[maxIndex:], p2[maxIndex:]
}
