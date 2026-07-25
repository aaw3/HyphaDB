package blockcache

type Key struct {
	TableID uint64
	Offset  uint64
}

type Cache interface {
	Get(key Key) ([]byte, bool)
	Set(key Key, block []byte)
	Delete(key Key)
	PurgeTable(tableID uint64)
}
