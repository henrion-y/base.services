package ac_automaton

import (
	"strconv"
	"testing"
	"time"
)

func addPatternAndBuild(t *testing.T, ac *AcAutoMachine) {
	for i := 0; i < 100000; i++ {
		go ac.AddPatternAndBuild(strconv.Itoa(i))
		t.Log(i)
	}
}

func query(t *testing.T, ac *AcAutoMachine) {
	for i := 0; i < 100000; i++ {
		go ac.Query(strconv.Itoa(i))
		t.Log(i)
	}
}

func TestAcAutoMachine_Query(t *testing.T) {
	ac := NewAcAutoMachine()
	err := ac.LoadPatterns([]string{"花儿", "这样", "红"})
	if err != nil {
		t.Log(err)
	}
	ac.StopLoad()

	text := `祖国的花朵为什么这样红？`

	content := text
	results := ac.Query(content)
	for _, result := range results {
		t.Log(result)
	}

	go addPatternAndBuild(t, ac)
	go query(t, ac)

	time.Sleep(30 * time.Second)
}
