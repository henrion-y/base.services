package ac_automaton

import (
	"errors"
	"sync"
)

type acNode struct {
	fail      *acNode
	isPattern bool
	next      map[rune]*acNode
}

func newAcNode() *acNode {
	return &acNode{
		fail:      nil,
		isPattern: false,
		next:      map[rune]*acNode{},
	}
}

// AcAutoMachine todo 看能不能减小锁的粒度提高并发能力
type AcAutoMachine struct {
	sync.RWMutex
	loadEnd bool
	root    *acNode
}

func NewAcAutoMachine() *AcAutoMachine {
	return &AcAutoMachine{
		root: newAcNode(),
	}
}

func (ac *AcAutoMachine) addPattern(pattern string) {
	chars := []rune(pattern)
	iter := ac.root
	for _, c := range chars {
		if _, ok := iter.next[c]; !ok {
			iter.next[c] = newAcNode()
		}
		iter = iter.next[c]
	}
	iter.isPattern = true
}

func (ac *AcAutoMachine) build() {
	var queue []*acNode
	queue = append(queue, ac.root)
	for len(queue) != 0 {
		parent := queue[0]
		queue = queue[1:]

		for char, child := range parent.next {
			if parent == ac.root {
				child.fail = ac.root
			} else {
				failNode := parent.fail
				for failNode != nil {
					if _, ok := failNode.next[char]; ok {
						child.fail = failNode.next[char]
						break
					}
					failNode = failNode.fail
				}
				if failNode == nil {
					child.fail = ac.root
				}
			}
			queue = append(queue, child)
		}
	}
}

func (ac *AcAutoMachine) Query(content string) (results []string) {
	ac.RLock()
	defer ac.RUnlock()

	chars := []rune(content)
	iter := ac.root
	var start, end int
	for i, c := range chars {
		_, ok := iter.next[c]
		for !ok && iter != ac.root {
			iter = iter.fail
		}
		if _, ok = iter.next[c]; ok {
			if iter == ac.root {
				start = i
			}
			iter = iter.next[c]
			if iter.isPattern {
				end = i
				results = append(results, string([]rune(content)[start:end+1]))
			}
		}
	}
	return
}

func (ac *AcAutoMachine) LoadPatterns(patterns []string) error {
	if ac.loadEnd {
		return errors.New("LoadEnd")
	}

	for i := range patterns {
		ac.addPattern(patterns[i])
	}
	return nil
}

func (ac *AcAutoMachine) StopLoad() {
	ac.loadEnd = true
	ac.build()
}

func (ac *AcAutoMachine) AddPatternAndBuild(pattern string) {
	ac.Lock()
	defer ac.Unlock()

	ac.addPattern(pattern)
	ac.build()
}
