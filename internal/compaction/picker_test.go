package compaction

import (
	"reflect"
	"testing"

	"github.com/aaw3/hyphadb/internal/manifest"
)

func TestPickL0CompactionWaitsForThreshold(t *testing.T) {
	tables := []manifest.SSTableMetadata{
		{ID: 1, Level: L0, SmallestKey: "apple", LargestKey: "banana"},
	}

	if _, ok := PickL0Compaction(tables, 2); ok {
		t.Fatal("picked compaction below threshold")
	}
}

func TestPickL0CompactionIncludesOverlappingL1Tables(t *testing.T) {
	tables := []manifest.SSTableMetadata{
		{ID: 1, Level: L0, SmallestKey: "banana", LargestKey: "carrot"},
		{ID: 2, Level: L0, SmallestKey: "apple", LargestKey: "banana"},
		{ID: 3, Level: L0 + 1, SmallestKey: "apple", LargestKey: "apricot"},
		{ID: 4, Level: L0 + 1, SmallestKey: "banana", LargestKey: "coconut"},
		{ID: 5, Level: L0 + 1, SmallestKey: "date", LargestKey: "fig"},
	}

	plan, ok := PickL0Compaction(tables, 2)
	if !ok {
		t.Fatal("did not pick L0 compaction at threshold")
	}
	if plan.SourceLevel != L0 || plan.TargetLevel != L0+1 {
		t.Fatalf("plan levels = %d -> %d, want 0 -> 1", plan.SourceLevel, plan.TargetLevel)
	}

	var got []uint64
	for _, table := range plan.Inputs {
		got = append(got, table.ID)
	}
	want := []uint64{1, 2, 3, 4}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("input IDs = %v, want %v", got, want)
	}
}

func TestPickL0CompactionConservativelyIncludesL1WithUnknownBounds(t *testing.T) {
	tables := []manifest.SSTableMetadata{
		{ID: 1, Level: L0, SmallestKey: "banana", LargestKey: "carrot"},
		{ID: 2, Level: L0 + 1, SmallestKey: "", LargestKey: ""},
		{ID: 3, Level: L0 + 1, SmallestKey: "zebra", LargestKey: "zucchini"},
	}

	plan, ok := PickL0Compaction(tables, 1)
	if !ok {
		t.Fatal("did not pick L0 compaction")
	}

	var got []uint64
	for _, table := range plan.Inputs {
		got = append(got, table.ID)
	}
	want := []uint64{1, 2, 3}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("input IDs = %v, want %v", got, want)
	}
}

func TestPickL0CompactionIgnoresOtherLevels(t *testing.T) {
	tables := []manifest.SSTableMetadata{
		{ID: 1, Level: L0, SmallestKey: "apple", LargestKey: "banana"},
		{ID: 2, Level: 2, SmallestKey: "apple", LargestKey: "banana"},
	}

	plan, ok := PickL0Compaction(tables, 1)
	if !ok {
		t.Fatal("did not pick L0 compaction")
	}
	if len(plan.Inputs) != 1 || plan.Inputs[0].ID != 1 {
		t.Fatalf("input tables = %+v, want only L0 table", plan.Inputs)
	}
}

func TestPickCompactionSelectsL1AndOverlappingL2Tables(t *testing.T) {
	tables := []manifest.SSTableMetadata{
		{ID: 1, Level: L0 + 1, SmallestKey: "apple", LargestKey: "banana"},
		{ID: 2, Level: L0 + 1, SmallestKey: "date", LargestKey: "fig"},
		{ID: 3, Level: L0 + 2, SmallestKey: "aardvark", LargestKey: "apricot"},
		{ID: 4, Level: L0 + 2, SmallestKey: "carrot", LargestKey: "coconut"},
		{ID: 5, Level: L0 + 2, SmallestKey: "zebra", LargestKey: "zucchini"},
	}

	plan, ok := PickCompaction(tables, L0+1, 2)
	if !ok {
		t.Fatal("did not pick L1 compaction at threshold")
	}
	if plan.SourceLevel != L0+1 || plan.TargetLevel != L0+2 {
		t.Fatalf("plan levels = %d -> %d, want 1 -> 2", plan.SourceLevel, plan.TargetLevel)
	}

	var got []uint64
	for _, table := range plan.Inputs {
		got = append(got, table.ID)
	}
	want := []uint64{1, 3}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("input IDs = %v, want %v", got, want)
	}
}

func TestPickCompactionWaitsForHigherLevelThreshold(t *testing.T) {
	tables := []manifest.SSTableMetadata{
		{ID: 1, Level: L0 + 1, SmallestKey: "apple", LargestKey: "banana"},
	}

	if _, ok := PickCompaction(tables, L0+1, 2); ok {
		t.Fatal("picked L1 compaction below threshold")
	}
}

func TestPickCompactionUsesHigherLevelByteBudget(t *testing.T) {
	tables := []manifest.SSTableMetadata{
		{ID: 1, Level: L0 + 1, SizeBytes: baseLevelTargetBytes},
	}

	plan, ok := PickCompaction(tables, L0+1, 100)
	if !ok {
		t.Fatal("did not pick L1 compaction at byte budget")
	}
	if plan.SourceLevel != L0+1 || plan.TargetLevel != L0+2 {
		t.Fatalf("plan levels = %d -> %d, want 1 -> 2", plan.SourceLevel, plan.TargetLevel)
	}
}

func TestPickCompactionPrefersLargestHigherLevelTable(t *testing.T) {
	tables := []manifest.SSTableMetadata{
		{ID: 1, Level: L0 + 1, SizeBytes: 10, SmallestKey: "apple", LargestKey: "banana"},
		{ID: 2, Level: L0 + 1, SizeBytes: 100, SmallestKey: "date", LargestKey: "fig"},
		{ID: 3, Level: L0 + 2, SizeBytes: 1, SmallestKey: "date", LargestKey: "fig"},
	}

	plan, ok := PickCompaction(tables, L0+1, 2)
	if !ok {
		t.Fatal("did not pick L1 compaction")
	}
	if len(plan.Inputs) != 2 || plan.Inputs[0].ID != 2 || plan.Inputs[1].ID != 3 {
		t.Fatalf("input tables = %+v, want largest L1 table and overlapping L2 table", plan.Inputs)
	}
}

func TestPickCompactionConservativelyIncludesUnknownL2Bounds(t *testing.T) {
	tables := []manifest.SSTableMetadata{
		{ID: 1, Level: L0 + 1, SmallestKey: "banana", LargestKey: "carrot"},
		{ID: 2, Level: L0 + 2, SmallestKey: "", LargestKey: ""},
		{ID: 3, Level: L0 + 2, SmallestKey: "zebra", LargestKey: "zucchini"},
	}

	plan, ok := PickCompaction(tables, L0+1, 1)
	if !ok {
		t.Fatal("did not pick L1 compaction")
	}

	var got []uint64
	for _, table := range plan.Inputs {
		got = append(got, table.ID)
	}
	want := []uint64{1, 2, 3}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("input IDs = %v, want %v", got, want)
	}
}
