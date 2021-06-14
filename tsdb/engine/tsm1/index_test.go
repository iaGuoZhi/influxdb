package tsm1

import (
	"github.com/influxdata/influxdb/v2/pkg/testing/assert"
	"testing"
)

func TestIndexInsertion(t *testing.T) {
	myIndex := createIndex()
	myIndex.addRecord(1, 5, 1)
	myIndex.addRecord(1, 3, 2)
	myIndex.addRecord(2, 6, 3)
	myIndex.addRecord(2, 2, 4)

	firstList := myIndex.get(1)
	first := firstList.head
	assert.Equal(t, first.value, uint64(3), "The two values should be the same.")
	second := first.next
	assert.Equal(t, second.value, uint64(5), "The two values should be the same.")
	secondList := myIndex.get(2)
	third := secondList.head
	assert.Equal(t, third.value, uint64(2), "The two values should be the same.")
	fourth := third.next
	assert.Equal(t, fourth.value, uint64(6), "The two values should be the same.")

	notExisting := myIndex.get(3)
	assert.Equal(t, notExisting, (*list)(nil), "No list should be returned.")
}
