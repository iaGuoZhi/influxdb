package tsm1

import (
	"math"
)

type index struct {
	setLsb uint64
	m map[uint64]*list
}

func createIndex() *index {
	return &index{
		setLsb: uint64(0x1f),
		m:      make(map[uint64] *list),
	}
}

func (i *index) addRecord(value float64, index uint64) {
	key := math.Float64bits(value) & i.setLsb
	//fmt.Printf("key: %v  %v  %v\n", key, i.setLsb, math.Float64bits(value))
	if list, ok := i.m[key]; ok {
		list.addRecord(value, index)
	} else {
		list := createList()
		list.addRecord(value, index)
		i.m[key] = list
	}
}

func (i *index) get(value float64, index uint64, size uint64) *record {
	key := math.Float64bits(value) & i.setLsb
	if list, ok := i.m[key]; ok {
		if list.head != nil && index- list.head.index < size {
			return list.head
		}
	}
	return nil
}
