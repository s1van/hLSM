#ifndef HLSM_FUNC_H
#define HLSM_FUNC_H

#include <string>
#include <cstdlib>
#include <vector>
#include "leveldb/hlsm_param.h"


#define FILE_HAS_SUFFIX(fname_, str_) ((fname_.find(str_) != std::string::npos))
#define HAS_SUBSTR(str_, sstr_) ((str_.find(sstr_) != std::string::npos))
#define PRIMARY_TO_SECONDARY_FILE(fname_) (( std::string(hlsm::config::secondary_storage_path) + fname_.substr(fname_.find_last_of("/")) ))
#define SECONDARY_TO_PRIMARY_FILE(fname_) (( std::string(hlsm::config::primary_storage_path) + fname_.substr(fname_.find_last_of("/")) ))

#define CALL_IF_HLSM(do_) do { if(hlsm::config::mode.ishLSM()) { DEBUG_INFO(3, "call if hlsm\n"); do_;} } while(0)

#define debug_detla_meta(meta_) \
	DEBUG_INFO(3, "start: %u, clear: %u, active: %u\n", \
			meta_->start, meta_->clear, meta_->active);

namespace hlsm {

inline static int table_name_to_number(const std::string& fname) {
	size_t n = fname.find("ldb");
	size_t m = fname.find_last_of("/");
	return std::atoi(fname.substr(m+1, n-1).c_str());
}

inline bool is_primary_file(const std::string& fname) {
	return HAS_SUBSTR(fname, hlsm::config::primary_storage_path);
}

inline static bool do_prefetch (int is_sequential) {
	return (is_sequential == 1);
}

inline static bool is_mirrored_write(uint64_t num, bool pure = false) {
	if (pure) {
		if (hlsm::runtime::table_level.withinPureMirroredLevel(num))
			return true;
	} else {
		if (hlsm::runtime::table_level.withinMirroredLevel(num))
			return true;
	}
	return false;
}

inline static bool is_mirrored_write(const std::string& fname, bool pure = false) {
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
			return is_mirrored_write(number, pure);
		}
	}
	return false;
}

inline bool read_from_primary(bool is_sequential) {
	return (is_sequential ? hlsm::runtime::seqential_read_from_primary : hlsm::runtime::random_read_from_primary);
}

inline static std::string relocate_file(const std::string& fname) {
	DEBUG_INFO(3, "relocate %s\n", fname.c_str());
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

// when cursor is used
inline double MaxBytesForLevel(int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.
  
  // Level 0 and Level 1 has the same size due to cursor, which is 
  //    determined by not the kL0_Size, but the kL0_StopWritesTrigger
  double result = leveldb::config::kL0_Size * 1048576.0;  

  // Level 2 and Level 3 now in fact has the size kL0_Size
  while (level > 3) {
    result *= leveldb::config::kLevelRatio;
    level = level - 2; // LX.L and LX.R have the same maximum size
  }
  return result;
}

/*
 * hLSM Level conversion
 * on_primary		on_secondary	logically	physical_pri	physical_sec
 * L0.L|L0.R		L0.L|L0.R		LL0			L1|L0			L1|L0
 * L1.L|L1.R		L1.NEW|…|R2|R1	LL1			L3|L2			L?|…|L3|L2
 * ......
 * X.L|X.R			…|R2|R1			LLX (end of 2-phase level)
 * X+1.L|X+1.R		X+1.L|X+1.R		LL<X+1>
 */

inline int get_logical_level(int original_level) {
	return original_level / 2;
}

inline int get_hlsm_new_level(int original_level) {
	int logical_level = get_logical_level(original_level);
	assert(logical_level >0 && logical_level <= hlsm::runtime::two_phase_end_level);

	return logical_level * (hlsm::runtime::delta_level_num + 1) + 1;
}

/*
 *  Within hlsm_util.cc
 */
int init_opq_helpler();

/*
 * DeltaLevelMeta
 */

inline uint32_t get_active_delta_level(delta_meta_t meta[], int llevel) {
	return llevel * (hlsm::runtime::delta_level_num + 1) + 1 - meta[llevel].active;
}

inline int get_pure_mirror_level(int level) {
	int lnum = hlsm::runtime::two_phase_end_level + 1;
	assert(level >= 2 * lnum);
	return get_hlsm_new_level(2 * lnum - 1)
			+ level - 2 * lnum;
}

inline std::vector<uint32_t> get_obsolete_delta_levels(delta_meta_t meta[], int llevel) {
	std::vector<uint32_t> levels;
	uint32_t start = meta[llevel].start;
	uint32_t clear = meta[llevel].clear;

	DEBUG_INFO(2, "start: %u, clear: %u\n", start, clear);
	// require: start and clear are set 0 by default
	while (start != clear) { // levels: start+1, _2, ..., clear
		start = start + 1;
		if (start > hlsm::runtime::delta_level_num) start = 1;
		levels.push_back(llevel * (hlsm::runtime::delta_level_num + 1) + 1 - start);
	}

	return levels;
}

/*
 * Bloom Filter
 */
inline size_t get_bloom_filter_probe_num (int bits_per_key) {
	int raw_probe_num = (hlsm::config::bloom_bits_use < bits_per_key &&
			hlsm::config::bloom_bits_use > 0)?
			hlsm::config::bloom_bits_use : bits_per_key;
	// We intentionally round down to reduce probing cost a little bit
	return static_cast<size_t>(raw_probe_num * 0.69);  // 0.69 =~ ln(2)
}

} // hlsm

#endif
