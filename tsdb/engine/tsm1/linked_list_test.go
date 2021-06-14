package tsm1

import (
	"fmt"
	"github.com/influxdata/influxdb/v2/pkg/testing/assert"
	"testing"
)

func TestListInsertion(t *testing.T) {
	myList := createList()
	fmt.Println("Created list")
	fmt.Println()

	fmt.Print("Adding records to the list...\n\n")
	myList.addRecord(5, 1)
	myList.addRecord(3, 2)
	myList.addRecord(6, 3)
	myList.addRecord(2, 4)

	first := myList.head
	assert.Equal(t, first.value, float64(2), "The two values should be the same.")
	second := first.next
	assert.Equal(t, second.value, float64(6), "The two values should be the same.")
	third := second.next
	assert.Equal(t, third.value, float64(3), "The two values should be the same.")
	fourth := third.next
	assert.Equal(t, fourth.value, float64(5), "The two values should be the same.")
}


func TestListNextRecordTtl(t *testing.T) {
	myList := createList()
	fmt.Println("Created list")
	fmt.Println()

	fmt.Print("Adding records to the list...\n\n")
	myList.addRecord(5, 1)
	myList.addRecord(3, 2)
	myList.addRecord(6, 33)
	myList.addRecord(2, 34)

	first := myList.head
	assert.Equal(t, first.value, float64(2), "The two values should be the same.")
	second := first.nextRecord(34, 32)
	assert.Equal(t, second.value, float64(6), "The two values should be the same.")
	third := second.nextRecord(34, 32)
	assert.Equal(t, third.value, float64(3), "The two values should be the same.")
	fourth := third.nextRecord(34, 32)
	assert.Equal(t, fourth, (*record)(nil), "No record should be returned.")
}