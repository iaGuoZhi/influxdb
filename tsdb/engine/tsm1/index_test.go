package tsm1

import (
	"github.com/influxdata/influxdb/v2/pkg/testing/assert"
	"testing"
)

func TestIndexInsertion(t *testing.T) {
	myIndex := createIndex()
	myIndex.addRecord(5.123456, 1)
	myIndex.addRecord(6.123456, 2)
	myIndex.addRecord(2.654321, 3)
	myIndex.addRecord(2.654321, 4)

	first := myIndex.get(5.123456, 5, 32)
	assert.Equal(t, first.value, 6.123456, "The two values should be the same.")
	second := first.nextRecord(5, 32)
	assert.Equal(t, second.value, 5.123456, "The two values should be the same.")
	third := myIndex.get(2.654321, 5, 32)
	assert.Equal(t, third.value, 2.654321, "The two values should be the same.")
	fourth := third.nextRecord(5, 32)
	assert.Equal(t, fourth.value, 2.654321, "The two values should be the same.")

	notExisting := myIndex.get(3, 5, 32)
	assert.Equal(t, notExisting, (*list)(nil), "No list should be returned.")
}
