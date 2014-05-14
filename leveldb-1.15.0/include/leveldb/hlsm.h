#ifndef HLSM_H
#define HLSM_H

#include <pthread.h>
#include <string>
#include <time.h>
#include <malloc.h>

#include "leveldb/hlsm_debug.h"
#include "leveldb/hlsm_types.h"

#define BLKSIZE 4096

#define HLSM_CPREFETCH false //for compaction
#define USE_OPQ_THREAD
#define COMPACT_SECONDARY_PWRITE


#define EXCLUDE_FILE(fname_, str_)	((fname_.find(str_) == std::string::npos))
#define EXCLUDE_FILES(fname_)	((MIRROR_ENABLE ? 	\
	EXCLUDE_FILE(fname_, "MANIFEST") && EXCLUDE_FILE(fname_, "CURRENT") 	\
	&& EXCLUDE_FILE(fname_, ".dbtmp") && EXCLUDE_FILE(fname_, "LOG") 	\
	&& EXCLUDE_FILE(fname_, ".log") && EXCLUDE_FILE(fname_, "LOCK") : 0 ))

//assume that .log will only be written, deleted, and renamed
//#define HLSM_HDD_ONLY(fname_)	((MIRROR_ENABLE ? 	\
	!EXCLUDE_FILE(fname_, ".log") : 0 ))		\

#define HLSM_HDD_ONLY(fname_)	0


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
extern int full_mirror;
extern int use_opq_thread;
extern const char *secondary_storage_path;
extern int compact_read_from_secondary;
}

namespace runtime {
extern pthread_t *compaction_helper;
extern opq mio_queue;
}

}

/*********************** Configuration (END) **************************/

#endif  //HLSM_H
