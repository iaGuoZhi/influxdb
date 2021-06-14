package tsm1

import "fmt"

const expiry = uint32(32)

type record struct {
	value   uint64
	ttl uint32
	next   *record
}

type list struct {
	head       *record
}

func createList() *list {
	return &list{}
}

func (p *list) addRecord(value uint64, ttl uint32) error {
	s := &record{
		value:   value,
		ttl:   ttl,
	}
	if p.head != nil {
		s.next = p.head
		p.head = s
	}
	p.head = s
	return nil
}

func (r *record) nextRecord(ttl uint32) *record {
	if r.next != nil && (ttl - r.next.ttl <= 32) {
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
