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
int use_opq_thread = 1;
char *secondary_storage_path = NULL;
int compact_read_from_secondary = 1;

int debug_level = 0;
char* debug_file = NULL;
}

namespace runtime {
pthread_t *compaction_helper = NULL;
opq mio_queue = NULL;

FILE *debug_fd = stderr;
}

}
