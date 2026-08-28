package compaction

import "github.com/aaw3/hyphadb/internal/manifest"

const L0 uint32 = 0

// CompactionPlan identifies the tables that should be merged and the level
// where the merged output will be placed.
type CompactionPlan struct {
	SourceLevel uint32
	TargetLevel uint32
	Inputs      []manifest.SSTableMetadata
}

// PickL0Compaction selects an L0-to-L1 compaction once L0 reaches threshold.
// All selected L0 tables are included because L0 tables may overlap. Only L1
// tables overlapping the combined L0 key range are included.
//
// Tables with missing bounds are handled conservatively: all L1 tables are
// included because excluding an unknown overlapping table would be unsafe.
func PickL0Compaction(
	tables []manifest.SSTableMetadata,
	threshold int,
) (CompactionPlan, bool) {
	if threshold <= 0 {
		return CompactionPlan{}, false
	}

	var l0 []manifest.SSTableMetadata
	for _, table := range tables {
		if table.Level == L0 {
			l0 = append(l0, table)
		}
	}

	if len(l0) < threshold {
		return CompactionPlan{}, false
	}

	plan := CompactionPlan{
		SourceLevel: L0,
		TargetLevel: L0 + 1,
		Inputs:      append([]manifest.SSTableMetadata(nil), l0...),
	}

	minKey, maxKey, knownRange := combinedRange(l0)
	allL1RangesKnown := true
	for _, table := range tables {
		if table.Level == L0+1 && !hasKeyRange(table) {
			allL1RangesKnown = false
			break
		}
	}

	for _, table := range tables {
		if table.Level != L0+1 {
			continue
		}

		if !knownRange || !allL1RangesKnown || rangesOverlap(
			minKey,
			maxKey,
			table.SmallestKey,
			table.LargestKey,
		) {
			plan.Inputs = append(plan.Inputs, table)
		}
	}

	return plan, true
}

func combinedRange(tables []manifest.SSTableMetadata) (string, string, bool) {
	if len(tables) == 0 {
		return "", "", false
	}

	minKey := tables[0].SmallestKey
	maxKey := tables[0].LargestKey
	if !hasKeyRange(tables[0]) {
		return "", "", false
	}

	for _, table := range tables[1:] {
		if !hasKeyRange(table) {
			return "", "", false
		}
		if table.SmallestKey < minKey {
			minKey = table.SmallestKey
		}
		if table.LargestKey > maxKey {
			maxKey = table.LargestKey
		}
	}

	return minKey, maxKey, true
}

func hasKeyRange(table manifest.SSTableMetadata) bool {
	return table.SmallestKey != "" && table.LargestKey != ""
}

func rangesOverlap(leftMin, leftMax, rightMin, rightMax string) bool {
	return leftMin <= rightMax && rightMin <= leftMax
}
