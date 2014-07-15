#ifndef HLSM_PARAM_H
#define HLSM_PARAM_H

#include "leveldb/hlsm_types.h"
#include "db/dbformat.h"
#include <set>

/************************** Constants *****************************/
#define BLKSIZE 4096
#define HLSM_LOGICAL_LEVEL_NUM leveldb::config::kNumLevels/2

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
extern bool preload_metadata;
extern int kMinKBPerSeek;
extern int kMaxLevel;

extern const char *primary_storage_path;	// primary path holds all the .ldb files
extern const char *secondary_storage_path;
extern bool direct_write_on_secondary;
extern bool secondary_use_buffer_file;
extern bool lazy_sync_on_secondary;
extern bool run_compaction;
extern bool iterator_prefetch;
extern bool raw_prefetch;
extern bool append_by_opq;

extern int debug_level;	// default value 0 is; info whose level is smaller or equal to debug_level will be displayed
extern char* debug_file;// where to dump the debug info

extern int bloom_bits_use; // allow user to probe less bits in bloom filter
} // config

namespace runtime {
extern leveldb::Env* env_;

extern pthread_t *opq_helper;
extern opq op_queue;
extern opq hop_queue; // for high priority operations

extern FILE *debug_fd;	// initialized using hlsm::config::debug_file (default: stderr)
extern leveldb::port::Mutex debug_mutex_;
extern hlsm::NamedCounter counters;

extern bool  use_opq_thread;
extern bool full_mirror;
extern int mirror_start_level;
extern int top_mirror_end_level;
extern int top_pure_mirror_end_level; // so mirrored file is not deleted at the same time
extern bool use_cursor_compaction;
extern bool delayed_buf_reset;

extern bool seqential_read_from_primary;
extern bool random_read_from_primary;
extern bool meta_on_primary;
extern bool log_on_primary;

extern int kMinBytesPerSeek;

/* mirror L0.L|L0.R
 * new, delta sub-levels for each original cursor level
 * last 2-phase level has no NEW sub-level
 * last level is full mirrored
 */
static const int delta_level_num = 16; // choose a large level_num to handle overflow
static const int kLogicalLevels = leveldb::config::kNumLevels / 2;
static const int kNumLazyLevels = 2 + (delta_level_num + 1) * (kLogicalLevels - 3) + delta_level_num + 2;
extern int two_phase_end_level;

extern TableLevel table_level;
extern std::set<uint64_t> moving_tables_; // tables move from primary to secondary during 2-phase compaction in hlsm-tree
} // runtime

} // hlsm


#endif

