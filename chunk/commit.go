package chunk

func NewCommitChunk(rootAddr uint64) []byte {
	return Pack(CommitType, []uint64{rootAddr}, nil)
}
