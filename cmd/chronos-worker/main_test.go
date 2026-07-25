package main

import (
	"math"
	"reflect"
	"testing"
)

func TestSplitValues(t *testing.T) {
	actual := splitValues(" beta,alpha,beta,, ")
	if !reflect.DeepEqual(actual, []string{"alpha", "beta"}) {
		t.Fatalf("unexpected values: %v", actual)
	}
}

func TestValidCredits(t *testing.T) {
	if validCredits(0) || !validCredits(1) || !validCredits(math.MaxUint32) || validCredits(uint64(math.MaxUint32)+1) {
		t.Fatal("unexpected credit validation")
	}
}

func TestValueSet(t *testing.T) {
	values := valueSet([]string{"fanout", "verify"})
	if len(values) != 2 {
		t.Fatalf("unexpected set: %v", values)
	}
	if _, exists := values["fanout"]; !exists {
		t.Fatal("fanout value missing")
	}
}
