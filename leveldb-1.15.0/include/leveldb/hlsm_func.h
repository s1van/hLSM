#ifndef HLSM_FUNC_H
#define HLSM_FUNC_H

#include <string>
#include <cstdlib>
#include "leveldb/hlsm_param.h"


#define FILE_HAS_SUFFIX(fname_, str_) ((fname_.find(str_) != std::string::npos))
#define PRIMARY_TO_SECONDARY_FILE(fname_) (( std::string(hlsm::config::secondary_storage_path) + fname_.substr(fname_.find_last_of("/")) ))

namespace hlsm {

inline static int table_name_to_number(const std::string& fname) {
	size_t n = fname.find("ldb");
	size_t m = fname.find_last_of("/");
	return std::atoi(fname.substr(m+1, n-1).c_str());
}

inline static bool do_prefetch (int is_sequential) {
	return (is_sequential == 1);
}

inline static bool is_mirrored_write(const std::string& fname) {
	DEBUG_INFO(2, "%s\n", fname.c_str());
	size_t n = fname.find("ldb");
	if (n != std::string::npos && hlsm::config::secondary_storage_path != NULL) {
		if (hlsm::runtime::full_mirror) {
			return true;
		} else {
			size_t m = fname.find_last_of("/");
			int number = std::atoi(fname.substr(m+1, n-1).c_str());
			DEBUG_INFO(2, "%s\t%d\n", fname.substr(m+1, n-1).c_str(), number);
			// requires that table_level is updated before this call
			if (hlsm::runtime::table_level.withinMirroredLevel(number))
				return true;
		}
	}
	return false;
}

inline bool read_from_primary(bool is_sequential) {
	return (is_sequential ? hlsm::runtime::seqential_read_from_primary : hlsm::runtime::random_read_from_primary);
}

inline static std::string relocate_file(const std::string& fname) {
	DEBUG_INFO(2, "relocate %s\n", fname.c_str());
	if (FILE_HAS_SUFFIX(fname, ".ldb")) {
		return fname;
	} else if (FILE_HAS_SUFFIX(fname, ".log") && !hlsm::runtime::log_on_primary) {
		return PRIMARY_TO_SECONDARY_FILE(fname);
	} else if (!hlsm::runtime::meta_on_primary) {
		return PRIMARY_TO_SECONDARY_FILE(fname);
	}
	return fname;
}


inline int64_t TotalFileSize(const std::vector<leveldb::FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

inline double MaxBytesForLevel(int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.
  double result = leveldb::config::kL0_Size * 1048576.0;  // Result for both level-0 and level-1
  while (level > 1) {
    result *= leveldb::config::kLevelRatio;
    level = level - 2; // LX.L and LX.R have the same maximum size
  }
  return result;
}
/*
 *  Within hlsm_util.cc
 */
int init_opq_helpler();

} // hlsm

#endif
