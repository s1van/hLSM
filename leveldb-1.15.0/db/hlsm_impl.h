#ifndef HLSM_IMPL_H
#define HLSM_IMPL_H

#include <map>
#include <set>
#include <vector>

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "db/filename.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "leveldb/env.h"
#include "leveldb/hlsm.h"

namespace hlsm {
namespace runtime {

int init();
int cleanup();
int preload_metadata(leveldb::VersionSet*);
} // runtime

/* cursor re-organizes the compaction.
 * It separates each level into two parts, left and right, which
 * 	contains files without overlapped key ranges. L0.L is unchanged, which implies that
 * 	files inside may overlap each other.
 * 		L0.L|L0.R
 * 		L1.L|L1.R
 * 		L2.L|L2.R
 * 		...
 *
 * For each compaction, a file is picked from LX.L and merged to
 * 	the L(X+1). The maximum size of L1.L and L1.R can still be calculated
 * 	by MaxBytesForLevel.
 *
 * In our implementation, we treat LX.L and LX.R as two adjacent levels,
 * 	L(2X) and L(2X-1). Then we modified the compaction and ratio -related procedures
 * 	to enforce the control of cursor.
 *
 * When LX.R is full, we expect LX.L empty. If so, we can then rename LX.R as LX.L,
 * 	and set LX.R empty.
 */

namespace cursor {

inline double calculate_compaction_score(int level, std::vector<leveldb::FileMetaData*> files[]) {
	assert(level > 0);
	double score = 0;
	if (files[level].size() == 0)
		return score;

	if (level % 2 == 1) { // LX.L << LX.R > L(X+1).R
		score = std::max( TotalFileSize(files[level]) / MaxBytesForLevel(level)	// LX.L
			,TotalFileSize(files[level-1]) / MaxBytesForLevel(level-1));		// LX.R
	} else { // LX.R < L(X-1).L >> LX.L
		if (TotalFileSize(files[level+1]) == 0) // LX.L
			score = TotalFileSize(files[level])/ MaxBytesForLevel(level);	// LX.R
		else
			score = 0; // this guarantees that LX.R won't be compacted since the score of LX.L is larger than 0
	}
	DEBUG_INFO(2, "level %d: score = %.4f, size = %ld, max_size = %.4f\n", level, score,
			TotalFileSize(files[level]), MaxBytesForLevel(level));
	return score;
}

inline double calculate_level0_compaction_score(size_t level0_size, size_t level1_size) {
	if (!hlsm::runtime::use_cursor_compaction) { // then use original calculation
		return level0_size /
				static_cast<double>(leveldb::config::kL0_CompactionTrigger);
	}

	double score = 0;
	if (!hlsm::config::mode.ishLSM() ||
			(level0_size >= leveldb::config::kL0_CompactionTrigger && level1_size == 0) ) // level 1 must be empty
		score = level0_size/static_cast<double>(leveldb::config::kL0_CompactionTrigger);

	DEBUG_INFO(2, "size: %lu\tscore: %.3f\n", level0_size, score);
	return score;
}

inline bool is_trivial_move(int level) {
	return ( !hlsm::runtime::use_cursor_compaction || level % 2 == 1);
}

inline bool is_whole_level_move(int level) {
	return (hlsm::runtime::use_cursor_compaction && level > 0 && level % 2 == 0); // level 0 must go through compaction
}

} // cursor

int delete_secondary_file(leveldb::Env* const, uint64_t);
int prefetch_file(leveldb::RandomAccessFile*, uint64_t);

} // namespace hlsm

namespace leveldb {

/*
 * Within BackgroundCompaction()
 */

inline int move_file_down(FileMetaData* f, VersionEdit* edit, int level) {
	DEBUG_INFO(2, "number: %lu\tlevel: %d\n", f->number, level);
	edit->DeleteFile(level, f->number);
	edit->AddFile(level + 1, f->number, f->file_size,
			f->smallest, f->largest);
	hlsm::runtime::table_level.add(f->number, level+1);
	if (level + 1 == hlsm::runtime::mirror_start_level) { // need to copy the content to secondary
		OPQ_ADD_COPYFILE(hlsm::runtime::op_queue,
				new std::string(TableFileName(hlsm::config::primary_storage_path, f->number)));
	}

	return 0;
}

} // namespace leveldb


#endif


