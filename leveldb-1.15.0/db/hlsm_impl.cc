#include <algorithm>

#include "leveldb/hlsm_types.h"
#include "leveldb/status.h"
#include "leveldb/hlsm.h"

#include "version_set.h"
#include "table_cache.h"
#include "db_impl.h"


namespace leveldb{


Status TableCache::PreLoadTable(uint64_t file_number, uint64_t file_size) {
	Cache::Handle* handle = NULL;
	Status s = FindTable(file_number, file_size, &handle);
	cache_->Release(handle);

	return s;
}

int Version::preload_metadata(int max_level) {
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

} // leveldb

namespace hlsm{
namespace runtime {

int preload_metadata(leveldb::VersionSet* versions) {
	return versions->current()->preload_metadata(hlsm::config::preload_metadata_max_level);
}

} // runtime
} // hlsm
