package tsm1

type index struct {
	m map[uint8]*list
}

func createIndex() *index {
	return &index{
		make(map[uint8] *list),
	}
}

func (i *index) addRecord(key uint8, value uint64, ttl uint32) {
	if list, ok := i.m[key]; ok {
		list.addRecord(value, ttl)
	} else {
		list := createList()
		list.addRecord(value, ttl)
		i.m[key] = list
	}
}

func (i *index) get(key uint8) *list {
	if list, ok := i.m[key]; ok {
		return list
	}
	return nil
}
