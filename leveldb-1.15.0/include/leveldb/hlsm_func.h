#ifndef HLSM_FUNC_H
#define HLSM_FUNC_H

#include <string>
#include <cstdlib>
#include "leveldb/hlsm_param.h"


#define FILE_HAS_SUFFIX(fname_, str_) ((fname_.find(str_) != std::string::npos))
#define PRIMARY_TO_SECONDARY_FILE(fname_) (( std::string(hlsm::config::secondary_storage_path) + fname_.substr(fname_.find_last_of("/")) ))

namespace hlsm {

inline static bool is_mirrored_write(const std::string& fname) {
	DEBUG_INFO(2, "%s\n", fname.c_str());
	size_t n = fname.find("ldb");
	size_t m = fname.find_last_of("/");
	if (n != std::string::npos)
		if (hlsm::runtime::full_mirror)
			return true;
		else {
			int number = std::atoi(fname.substr(m+1, n-1).c_str());
			DEBUG_INFO(2, "%s\t%d\n", fname.substr(m+1, n-1).c_str(), number);
			// requires that table_level is updated before this call
			if (hlsm::runtime::table_level.withinMirroredLevel(number))
				return true;
		}
	return false;
}

} // hlsm

#endif
