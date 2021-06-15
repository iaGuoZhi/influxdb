package tsm1

import "fmt"

type record struct {
	value   float64
	index   uint64
	next    *record
}

type list struct {
	head       *record
}

func createList() *list {
	return &list{}
}

func (p *list) addRecord(value float64, index uint64) error {
	s := &record{
		value:   value,
		index:   index,
	}
	if p.head != nil {
		s.next = p.head
		p.head = s
	}
	p.head = s
	return nil
}

func (r *record) nextRecord(index uint64, size uint64) *record {
	if r.next != nil && (index - r.next.index < size) {
		return r.next
	}
	return nil
}

func (p *list) showAllRecords() error {
	currentNode := p.head
	if currentNode == nil {
		fmt.Println("List is empty.")
		return nil
	}
	fmt.Printf("%+v\n", *currentNode)
	for currentNode.next != nil {
		currentNode = currentNode.next
		fmt.Printf("%+v\n", *currentNode)
	}

	return nil
}
