#include "leveldb/hlsm_types.h"
#include <pthread.h>
#include <stdio.h>

namespace leveldb {

namespace config {
int kTargetFileSize = 2 * 1048576;
int kL0_Size = 10;       // in MB
int kLevelRatio = 10;    // enlarge the level size ten times when the db levels up
}

uint32_t FileNameHash::hash[] = {0};
}

namespace hlsm {

namespace config {
int full_mirror = 0;
bool use_opq_thread = true;
int preload_metadata_max_level = 4;

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

int init() {
	if (hlsm::config::debug_file != NULL)
		debug_fd = fopen(hlsm::config::debug_file, "w");
	return 0;
}

} // runtime

} // hlsm
