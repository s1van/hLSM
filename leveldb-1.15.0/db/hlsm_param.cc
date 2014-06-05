#include "leveldb/hlsm_types.h"
#include <pthread.h>
#include <stdio.h>

namespace leveldb {

namespace config {
int kTargetFileSize = 2 * 1048576;
int kL0_Size = 10;       // in MB
int kLevelRatio = 10;    // enlarge the level size ten times when the db levels up
}

}

namespace hlsm {

namespace config {
DBMode mode("Default");
bool use_opq_thread = true;
int preload_metadata_max_level = 4;

char *primary_storage_path = NULL;
char *secondary_storage_path = NULL;
bool compact_read_from_secondary = true;
bool direct_write_on_secondary = true;
bool secondary_use_buffer_file = true;
bool lazy_sync_on_secondary = true;

int debug_level = 0;
char* debug_file = NULL;
} //config

namespace runtime {
pthread_t *opq_helper = NULL;
opq op_queue = NULL;
FILE *debug_fd = stderr;

bool full_mirror = false;
int mirror_start_level = 1028;
bool use_cursor_compaction = false;

bool seqential_read_from_primary = true;
bool random_read_from_primary = true;
bool meta_on_primary = true;
bool log_on_primary = true;

TableLevel table_level;
uint32_t FileNameHash::hash[] = {0};
} // runtime

} // hlsm
