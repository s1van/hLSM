#ifndef HLSM_IMPL_H
#define HLSM_IMPL_H

#include <map>
#include <set>
#include <vector>

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "db/filename.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "leveldb/env.h"

namespace hlsm {
namespace runtime {

int init();
int cleanup();
int preload_metadata(leveldb::VersionSet*);
} // runtime

/* cursor re-organizes the compaction.
 * It separates each level into two parts, left and right, which
 * 	contains files without overlapped key ranges. L0 is unchanged.
 * 		L0
 * 		L1.L|L1.R
 * 		L2.L|L2.R
 * 		...
 *
 * For each compaction, a file is picked from LX.L and merged to
 * 	the L(X+1). The maximum size of L1.L and L1.R can still be calculated
 * 	by MaxBytesForLevel.
 *
 * In our implementation, we treat LX.L and LX.R as two adjacent levels,
 * 	L(2X) and L(2X-1). Then we modified the compaction and ratio -related procedures
 * 	to enforce the control of cursor.
 *
 * When LX.R is full, we expect LX.L empty. If so, we can then rename LX.R as LX.L,
 * 	and set LX.R empty.
 */

namespace cursor {

double calculate_compaction_score(int, std::vector<leveldb::FileMetaData*> *);

} // cursor

int delete_secondary_file(leveldb::Env* const, uint64_t);

} // hlsm


#endif


