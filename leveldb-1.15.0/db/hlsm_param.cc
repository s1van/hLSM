#include "leveldb/hlsm_types.h"
#include <pthread.h>
#include <stdio.h>
#include <set>

namespace leveldb {

namespace config {
int kTargetFileSize = 2 * 1048576;
int kL0_Size = 10;       // in MB
int kLevelRatio = 10;    // enlarge the level size ten times when the db levels up
int kMaxMemCompactLevel = 2;
}

}

namespace hlsm {

namespace config {
DBMode mode("Default");
bool preload_metadata = 1;
int kMinKBPerSeek = 16;
int kMaxLevel = -1;

const char *primary_storage_path = NULL;
const char *secondary_storage_path = NULL;
bool direct_write_on_secondary = true;
bool secondary_use_buffer_file = true;
bool lazy_sync_on_secondary = false;
bool run_compaction = true;
bool iterator_prefetch = false; // not good yet
bool append_by_opq = false;

int debug_level = 0;
char* debug_file = NULL;

int bloom_bits_use = -1;
} //config

namespace runtime {
leveldb::Env* env_ = NULL;

pthread_t *opq_helper = NULL;
opq op_queue = NULL;
opq hop_queue = NULL;

FILE *debug_fd = stderr;
leveldb::port::Mutex debug_mutex_;

bool use_opq_thread = false;
bool full_mirror = false;
int mirror_start_level = 1028;
int top_mirror_end_level = -1;
int top_pure_mirror_end_level = -1;
bool use_cursor_compaction = false;

bool seqential_read_from_primary = true;
bool random_read_from_primary = true;
bool meta_on_primary = true;
bool log_on_primary = true;

int kMinBytesPerSeek = 16384;

int two_phase_end_level = 0;

TableLevel table_level;
uint32_t FileNameHash::hash[] = {0};
std::set<uint64_t> moving_tables_;

} // runtime

} // hlsm
