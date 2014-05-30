#include <algorithm>

#include "leveldb/hlsm_types.h"
#include "leveldb/status.h"
#include "leveldb/hlsm.h"

#include "version_set.h"
#include "version_edit.h"
#include "port/port_posix.h"

#include "table_cache.h"
#include "db_impl.h"


namespace leveldb{


Status TableCache::PreLoadTable(uint64_t file_number, uint64_t file_size) {
	Cache::Handle* handle = NULL;
	Status s = FindTable(file_number, file_size, &handle);
	cache_->Release(handle);

	return s;
}

int Version::PreloadMetadata(int max_level) {
	DEBUG_INFO(1, "Start\n");
	for (int level = 0; level < std::max(config::kNumLevels, max_level); level++) {
		size_t num_files = files_[level].size();
		FileMetaData* const* files = &files_[level][0];
		for (uint32_t i = 0; i < num_files; i++) {
			Status s = vset_->table_cache_->PreLoadTable(files[i]->number, files[i]->file_size);
		}

	}
	DEBUG_INFO(1, "End\n");
	return 0;
}


Status VersionSet::MoveLevelDown(Compaction* c, port::Mutex *mutex_){
    assert(c->num_input_files(1) == 0);
    int level = c->level();
    leveldb::FileMetaData* const* files = &this->current()->files_[level][0];
    size_t num_files = this->current()->files_[level].size();
    DEBUG_INFO(2, "move %lu files from level %d to level %d\n", num_files, level, level+1);

    for(int i = 0; i < num_files; i++) {
    	leveldb::FileMetaData* f = files[i];
    	c->edit()->DeleteFile(level, f->number);
    	c->edit()->AddFile(level + 1, f->number, f->file_size,
    	                       f->smallest, f->largest);
    	DEBUG_INFO(3, "[%d/%lu] number: %lu\t size: %lu\n", i+1, num_files, f->number, f->file_size);
    }

    leveldb::Status status = this->LogAndApply(c->edit(), mutex_);
    return status;
}

} // leveldb

namespace hlsm{
namespace runtime {

int preload_metadata(leveldb::VersionSet* versions) {
	return versions->current()->PreloadMetadata(hlsm::config::preload_metadata_max_level);
}


int init() {
	if (hlsm::config::debug_file != NULL)
		debug_fd = fopen(hlsm::config::debug_file, "w");
	DEBUG_INFO(1, "debug_fd = %p\tstderr = %p\n", debug_fd, stderr);

	if (hlsm::config::mode.isDefault()) {

	} else if (hlsm::config::mode.isFullMirror()) {
		full_mirror = true;
		full_mirror_start_level = 0;
	} else if (hlsm::config::mode.isbLSM()) {
		full_mirror = false;
		use_cursor_compaction = true;
	} else if (hlsm::config::mode.ishLSM()) {
		full_mirror = true;
		full_mirror_start_level = 6; // logically the last level
		use_cursor_compaction = true;
	}

	return 0;
}

int cleanup() {
	DEBUG_INFO(1, "debug_fd = %p\tstderr = %p\n", debug_fd, stderr);
	if (debug_fd != stderr)
		fclose(debug_fd);
	return 0;
}

} // runtime

namespace cursor {
static double MaxBytesForLevel(int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.
  double result = leveldb::config::kL0_Size * 1048576.0;  // Result for both level-0 and level-1
  while (level > 1) {
    result *= leveldb::config::kLevelRatio;
    level = level - 2; // LX.L and LX.R have the same maximum size
  }
  return result;
}

static int64_t TotalFileSize(const std::vector<leveldb::FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

double calculate_compaction_score(int level, std::vector<leveldb::FileMetaData*> files[]) {
	assert(level > 0);
	double score = 0;
	if (level % 2 == 0) { // LX.L << LX.R > L(X+1).R
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

} // cursor

} // hlsm
