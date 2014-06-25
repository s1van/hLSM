#ifndef HLSM_PARAM_H
#define HLSM_PARAM_H

#include "leveldb/hlsm_types.h"
#include "db/dbformat.h"
#include <set>

/************************** Constants *****************************/
#define BLKSIZE 4096

/************************** Configuration *****************************/
namespace leveldb{

namespace config {
extern int kTargetFileSize;
extern int kL0_Size;     // in MB
extern int kLevelRatio;  // enlarge the level size by ten when the db levels up
}

}

namespace hlsm {

namespace config {
extern DBMode mode;
extern int preload_metadata_max_level;
extern int kMinKBPerSeek;

extern const char *primary_storage_path;	// primary path holds all the .ldb files
extern const char *secondary_storage_path;
extern bool compact_read_from_secondary;
extern bool direct_write_on_secondary;
extern bool secondary_use_buffer_file;
extern bool lazy_sync_on_secondary;

extern int debug_level;	// default value 0 is; info whose level is smaller or equal to debug_level will be displayed
extern char* debug_file;// where to dump the debug info
} // config

namespace runtime {
extern pthread_t *opq_helper;
extern opq op_queue;
extern FILE *debug_fd;	// initialized using hlsm::config::debug_file (default: stderr)

extern bool  use_opq_thread;
extern bool full_mirror;
extern int mirror_start_level;
extern int top_mirror_end_level;
extern bool use_cursor_compaction;

extern bool seqential_read_from_primary;
extern bool random_read_from_primary;
extern bool meta_on_primary;
extern bool log_on_primary;

extern int kMinBytesPerSeek;

// new, and 4 (size ratio) delta sub-levels; last level has one less sub-level
static const int kNumLazyLevels = 2 + 5 * (leveldb::config::kNumLevels/2 - 2) + 4;
extern int two_phase_end_level;

extern TableLevel table_level;
extern std::set<uint64_t> moving_tables_; // tables move from primary to secondary during 2-phase compaction in hlsm-tree
} // runtime

} // hlsm

#endif

