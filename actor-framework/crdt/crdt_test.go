package crdt

import (
	"testing"
	"time"
)

func TestIncrement_GCount(t *testing.T) {
	gCounter := NewGCounter("Node1")
	gCounter.Increment()
	if gCounter.LocalValue() != 1 {
		t.Errorf("Nije jednom inkrementovan cvor, value = %d", gCounter.LocalValue())
	}
}

func TestMerge_GCount(t *testing.T) {
	gCounter := NewGCounter("Node1")
	gCounter.Increment()
	gCounter2 := NewGCounter("Node2")
	gCounter2.Increment()
	gCounter2.Increment()

	gCounter.Merge(gCounter2)

	if gCounter.Value() != 3 {
		t.Errorf("Nije dobro merge-ovan, value = %d", gCounter.Value())
	}

}

func TestIncrementBy_GCount(t *testing.T) {
	gCounter := NewGCounter("Node1")
	gCounter.IncrementBy(3)

	if gCounter.Value() != 3 {
		t.Errorf("Nije dobro merge-ovan, value = %d", gCounter.Value())
	}

}

func TestSerialization_GCount(t *testing.T) {
	gCounter := NewGCounter("Node1")
	gCounter.IncrementBy(3)

	payload, err := gCounter.MarshalJSON()
	if err != nil {
		t.Error(err.Error())
	}

	gCounter2 := NewGCounter("Node2")
	gCounter2.Increment()
	err = gCounter2.UnmarshalAndMerge(payload)

	if err != nil {
		t.Error(err.Error())
	}

	if gCounter2.Value() != 4 {
		t.Errorf("Nije dobro merge-ovan, value = %d", gCounter.Value())
	}

}

func TestSet_LWWRegister(t *testing.T) {
	lwwRegister := NewLWWRegister[int]()
	lwwRegister.Set(3)
	if lwwRegister.Get() != 3 {
		t.Errorf("Nije pravilno setovana vrednost 3, dobijena vrednost je: %d", lwwRegister.Get())
	}
}

func TestMergeNewTimestamp_LWWRegister(t *testing.T) {
	lwwRegister := NewLWWRegister[int]()
	lwwRegister.Set(3)

	time.Sleep(1 * time.Millisecond)

	lwwRegister2 := NewLWWRegister[int]()
	lwwRegister2.Set(6)

	lwwRegister.Merge(lwwRegister2)
	if lwwRegister.Get() != 6 {
		t.Errorf("Nije pravilno mergovana vrednost, dobijena vrednost je: %d", lwwRegister.Get())
	}
}

func TestMergeOldTimestamp_LWWRegister(t *testing.T) {
	lwwRegister := NewLWWRegister[int]()
	lwwRegister.Set(3)

	time.Sleep(1 * time.Millisecond)

	lwwRegister2 := NewLWWRegister[int]()
	lwwRegister2.Set(6)

	lwwRegister2.Merge(lwwRegister)
	if lwwRegister2.Get() != 6 {
		t.Errorf("Nije pravilno mergovana vrednost, dobijena vrednost je: %d", lwwRegister.Get())
	}
}

func TestAdd_GSet(t *testing.T) {
	gSet := NewGSet[int]()
	gSet.Add(3)
	if !gSet.Contains(3) {
		t.Errorf("gSet ne sadrzi dodatu vrednost")
	}
}

func TestMerge_GSet(t *testing.T) {
	gSet := NewGSet[int]()
	gSet.Add(3)

	gSet2 := NewGSet[int]()
	gSet2.Add(5)

	gSet.Merge(gSet2)

	if gSet.Size() != 2 {
		t.Errorf("Nije pravilno mergovano, ne sadrzi dva elementa, vec: %d", gSet.Size())
	}
}

func TestAdd_ORSet(t *testing.T) {
	orSet := NewORSet[int]()
	orSet.Add(3)
	if !orSet.Contains(3) {
		t.Errorf("orSet ne sadrzi dodatu vrednost")
	}
}

func TestRemove_ORSet(t *testing.T) {
	orSet := NewORSet[int]()
	orSet.Add(3)
	orSet.Remove(3)
	if orSet.Contains(3) {
		t.Errorf("orSet ne sadrzi dodatu vrednost")
	}
}

func TestConcurentAdd_ORSet(t *testing.T) {
	orSet := NewORSet[int]()
	orSet.Add(3)
	orSet.Remove(3)
	orSet.Add(3)
	if !orSet.Contains(3) {
		t.Errorf("orSet ne sadrzi dodatu vrednost")
	}
}

func TestMerge_ORSet(t *testing.T) {
	orSet := NewORSet[int]()
	orSet.Add(3)
	orSet.Remove(3)
	orSet.Add(3)

	orSet2 := NewORSet[int]()
	orSet2.Add(4)

	orSet2.Merge(orSet)
	if orSet2.Size() != 2 {
		t.Errorf("orSet ne radi merge treba da sadrzi dva el, a sadrzi: %d", orSet.Size())
	}
}
