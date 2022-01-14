package tsm1

import (
	"math"
	"math/bits"
)

const positions = previousValues / 16
type index struct {
	setLsb uint64
	//array [32]*list.List
	array [32]*myList
	values [32][positions]float64
	indices [32][positions]uint64
	pointers [32]uint8
}

func createIndex() *index {
	return &index{
		setLsb: uint64(31),
	}
}

func (i *index) addRecord(value float64, index uint64) {
	key := math.Float64bits(value) & i.setLsb
	i.values[key][i.pointers[key]] = value
	i.indices[key][i.pointers[key]] = index
	//fmt.Printf("Adding %v with index %v to key %v\n", value, index, key)
	i.pointers[key] = (i.pointers[key] + 1) % positions
	/*if i.array[key] == nil {
		newList := createList()
		newList.addRecord(value, index)
		i.array[key] = newList
		//fmt.Printf("Adding %v with index %v to key %v\n", value, index, key)
	} else {
		i.array[key].addRecord(value, index)
		//fmt.Printf("Adding %v with index %v to key %v\n", value, index, key)
	}*/
}

func (i *index) getAll(value float64, index uint64, size uint64, threshold uint64) uint64 {
	maxTrailingBits := threshold
	previousIndex := uint64(size)
	key := math.Float64bits(value) & i.setLsb
	pointer := (i.pointers[key] - 1) % positions
	currIndex := i.indices[key][pointer]
	currValue := i.values[key][pointer]
	elementsChecked := 0
	//fmt.Printf("Checking: %v = %v = %v\n", currIndex, currValue, index)
	for index - currIndex < size && elementsChecked < positions {
		//fmt.Printf("Checking: %v = %v\n", currIndex, currValue)
		iVDelta := math.Float64bits(value) ^ math.Float64bits(currValue)
		trailingBits := uint64(bits.TrailingZeros64(iVDelta))
		//fmt.Printf("Checking Index: %d trailing: %d, %064b\n", record.index, trailingBits, iVDelta)
		if trailingBits > maxTrailingBits {
			previousIndex = currIndex % size
			maxTrailingBits = trailingBits
		}
		pointer = (pointer -1) % positions
		currIndex = i.indices[key][pointer]
		currValue = i.values[key][pointer]
		elementsChecked++
	}


	/*l := i.array[key]
	if l == nil {
		//fmt.Printf("Previous: %v\n", previousIndex)
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
		record = record.nextRecord(index, size)
	}*/
	//fmt.Printf("Previous: %v  %v\n", previousIndex, elementsChecked)
	return previousIndex
}

