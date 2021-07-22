package tsm1

import (
	"math"
	"math/bits"
)

type index struct {
	setLsb uint64
	//array [32]*list.List
	array [32]*myList
}

func createIndex() *index {
	return &index{
		setLsb: uint64(0x1f),
	}
}

func (i *index) addRecord(value float64, index uint64) {
	key := math.Float64bits(value) & i.setLsb
	if i.array[key] == nil {
		newList := createList()
		newList.addRecord(value, index)
		/*newList := list.New()
		newList.PushFront(&record{
			value:   value,
			index:   index,
		})*/
		i.array[key] = newList
	} else {
		oldList := i.array[key]
		oldList.addRecord(value, index)
		/*oldList.PushFront(&record{
			value:   value,
			index:   index,
		})*/
		i.array[key] = oldList
	}
	//fmt.Printf("key: %v  %v  %v\n", key, i.setLsb, math.Float64bits(value))
	/*if list, ok := i.m[key]; ok {
		list.addRecord(value, index)
	} else {
		list := createList()
		list.addRecord(value, index)
		i.m[key] = list
	}*/
}

/*
func (i *index) get(value float64, index uint64, size uint64) *record {
	key := math.Float64bits(value) & i.setLsb
	if i.array[key] != nil {
		list := i.array[key]
		if list.head != nil && index - list.head.index < size {
			return list.head
		}
	}
	//if list, ok := i.m[key]; ok {
	//	if list.head != nil && index- list.head.index < size {
	//		return list.head
	//	}
	//}
	return nil
}*/

func (i *index) getAll(value float64, index uint64, size uint64) uint64 {
	maxTrailingBits := uint64(0)
	previousIndex := uint64(size)
	key := math.Float64bits(value) & i.setLsb
	l := i.array[key]
	if l == nil {
		return  previousIndex
	}

	record := l.head
	for record != nil {
		iVDelta := math.Float64bits(value) ^ math.Float64bits(record.value)
		trailingBits := uint64(bits.TrailingZeros64(iVDelta))
		//fmt.Printf("Checking Index: %d trailing: %d, %064b\n", record.index, trailingBits, iVDelta)
		if trailingBits > maxTrailingBits {
			previousIndex = record.index % size
			maxTrailingBits = trailingBits
		}
		if trailingBits == 64 {
			break
		}
		record = record.nextRecord(index, size)
	}


	/*for e := l.Front(); e != nil; e = e.Next() {
		if index - e.Value.(*record).index >= size {
			break
		}
		iVDelta := math.Float64bits(value) ^ math.Float64bits(e.Value.(*record).value)
		trailingBits := uint64(bits.TrailingZeros64(iVDelta))
		//fmt.Printf("Checking Index: %d trailing: %d, %064b\n", record.index, trailingBits, iVDelta)
		if trailingBits > maxTrailingBits {
			previousIndex = e.Value.(*record).index % size
			maxTrailingBits = trailingBits
		}
		if trailingBits == 64 {
			break
		}
	}*/

	return previousIndex
}

