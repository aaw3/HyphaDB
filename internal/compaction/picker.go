package compaction

import (
	"os"

	"github.com/aaw3/hyphadb/internal/manifest"
)

const L0 uint32 = 0

const baseLevelTargetBytes uint64 = 64 * 1024 * 1024

// CompactionPlan identifies the tables that should be merged and the level
// where the merged output will be placed.
type CompactionPlan struct {
	SourceLevel uint32
	TargetLevel uint32
	Inputs      []manifest.SSTableMetadata
}

// PickCompaction selects a compaction for sourceLevel once that level reaches
// threshold. L0 is handled specially because its tables may overlap: all L0
// tables are selected and overlapping tables from L1 are added. Higher levels
// select one source table and add overlapping tables from the next level.
//
// Tables with missing bounds are handled conservatively: all tables from the
// target level are included because excluding an unknown overlapping table
// would be unsafe.
func PickCompaction(
	tables []manifest.SSTableMetadata,
	sourceLevel uint32,
	threshold int,
) (CompactionPlan, bool) {
	if threshold <= 0 {
		return CompactionPlan{}, false
	}

	var source []manifest.SSTableMetadata
	for _, table := range tables {
		if table.Level == sourceLevel {
			source = append(source, table)
		}
	}

	if sourceLevel == L0 && len(source) < threshold {
		return CompactionPlan{}, false
	}
	if sourceLevel > L0 && len(source) < threshold &&
		totalTableBytes(source) < levelTargetBytes(sourceLevel) {
		return CompactionPlan{}, false
	}

	plan := CompactionPlan{
		SourceLevel: sourceLevel,
		TargetLevel: sourceLevel + 1,
	}

	selected := source
	if sourceLevel != L0 {
		// Higher levels are maintained as non-overlapping runs. Selecting one
		// table keeps each compaction bounded while still preserving the
		// overlap invariant with the next level.
		selected = source[:1]
	}
	plan.Inputs = append(plan.Inputs, selected...)

	minKey, maxKey, knownRange := combinedRange(selected)
	targetLevel := sourceLevel + 1
	allTargetRangesKnown := true
	for _, table := range tables {
		if table.Level == targetLevel && !hasKeyRange(table) {
			allTargetRangesKnown = false
			break
		}
	}

	for _, table := range tables {
		if table.Level != targetLevel {
			continue
		}

		if !knownRange || !allTargetRangesKnown || rangesOverlap(
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

// PickL0Compaction is kept as a convenience wrapper for callers that
// explicitly request the L0 compaction policy.
func PickL0Compaction(
	tables []manifest.SSTableMetadata,
	threshold int,
) (CompactionPlan, bool) {
	return PickCompaction(tables, L0, threshold)
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

func levelTargetBytes(level uint32) uint64 {
	if level == L0 {
		return 0
	}

	target := baseLevelTargetBytes
	for current := uint32(1); current < level; current++ {
		if target > ^uint64(0)/10 {
			return ^uint64(0)
		}
		target *= 10
	}
	return target
}

func totalTableBytes(tables []manifest.SSTableMetadata) uint64 {
	var total uint64
	for _, table := range tables {
		size := table.SizeBytes
		if size == 0 {
			if info, err := os.Stat(table.Path); err == nil && info.Size() > 0 {
				size = uint64(info.Size())
			}
		}
		if ^uint64(0)-total < size {
			return ^uint64(0)
		}
		total += size
	}
	return total
}

func rangesOverlap(leftMin, leftMax, rightMin, rightMax string) bool {
	return leftMin <= rightMax && rightMin <= leftMax
}
