hLSM 
==== 
 
LSM-tree variant for hybrid storage 
 
## db_bench Tests 
Create by 'make db_bench'. 
 
Script tests/test.sh wraps the db_bench and exposes most useful arguments. Scripts run_xxx.sh under tests/ further  
wraps test.sh for different type of tests. Detailed augument usage could be found in include/leveldb/hlsm_param.h.  
Some important arguments are listed below. 
 
### --mode 
*mode* specifies the type of leveldb instance we are gonna launch. There are four types for now. Different mode may require different input arguments. 
 
| mode          | description                 | arguments required   | 
| ------------- |:--------------------------  | -------------------- | 
| Default       | original leveldb            |                      | 
| FullMirror    | mirrored original leveldb (two storages)   |secondary_storage_path| 
| bLSM | leveldb with cursor compaction       | | 
| hLSM | leveldb with cursor compaction on HDD and two-phase compaction on SSD| secondary_storage_path| 
 
### --preload_metadata 
Preload all tables's metadata when set to 1. 
 
## debug 
To enable debug mode, comment out '-DNDEBUG' at the beginning of leveldb-1.5.0/Makefile. 
 
## db_gen 
Create by 'make db_gen'. 
 
*db_gen* is used to generate large multi-level db instance without compaction. So the time cost due to compaction-induced write amplification is totally avoided. It has arguments similar to db_bench. 
 
 
## Code Description 
The implementation of hLSM-tree is based on the elegant LSM-tree implementaion, levelDB. Here we briefly introduce the new files and existing files that are heavily adapted for hLSM-tree.  
 
### db/db_bench.cc 
We modified db_bench to support workloads of various read/write request mixture. Also, the benchmark is now compatible with YCSB data format.  
 
### db/db_gen.cc 
The db_gen hacks the internal strucutre of leveldb, so it is able to generate a multi-level database without performing compaction.  
 
### db/db_impl.cc 
The primary effort in the code is to make the recovery procedure and compaction precedure compatible with hLSM-tree (as well as blSM-tree and mirroring). 
 
### db/hlsm_impl.cc 
The file includes functions that address the following aspects: (1) SST Table metadata preloading (Table extension), (2) procedures related to cursor-based compactions (BasicVersionSet), (3) db_gen related internal functions, (4) miscellaneous functions for hLSM-tree, and (5) initialization (hlsm.runtime.init) and cleanup procedure.  
 
### db/hlsm_impl.h 
In this file, we adjusted the calculation of compaction level score for each type of the tree. 
 
### db/lazy_version_edit.(cc|h) 
It contains VersionEdit class adpated for hLSM-tree. During a compaction, a VersionEdit is created to record modifications to a Version instance. At the end of the compaction, the VersionEdit instance will be merged with the latest Version instance to create a newer Version. 
 
### db/lazy_version_set.(cc|h) 
It contains VersionSet class adpated for hLSM-tree. Each DB instance has a single VersionSet class which contains several Version instances. One Version is the latest Version. The others are Version instances that are still referred by user thread or compaction thread.  
 
### db/table_cache.cc 
The modifications in this file majorly address the mirrored table, which has two copies on two storage separately. We make sure that (1) there is only one cached content related to the two copies and (2) access to which copy is based on the policy pre-set during the initialization procedure. 
 
### db/version_(edit|set).(cc|h) 
Effort in these files separate the interface and implementation of VersionEdit and VersionSet. Consequently, it allows us to dynamically instantialize VersionSet for hLSM-tree or LSM-tree according to the give mode. 
 
### include/leveldb/hlsm_debug.h
This header contains debug function/macro wrappers. A typical debug wrapper has the form DEBUG\_XX(level, do) where level is an unsigned integer. When a hLSM instance is launched, user can set a global variable, debug\_level, so every debug wrapper call whose level is smaller than the debug\_level will be executed. In addition, a counter can be set with the wrapper DEBUG\_MEASURE\_RECORD(level\_, func\_, tag) where the string tag is the counter's name/id. The specified counter records the total time on evaluating func_ and the number of calls to func_. On the exit of the hLSM-tree instance, we will print the content of all active counters to the debug file (default: /tmp/hlsm\_log).

### include/leveldb/hlsm_func.h
This header provides utilities to (1) map between logical levels and physical levels in hLSM-tree (more details in section hLSM-tree level conversion) and (2) locate and check primary copy or secondary copy for mirrored table.

### include/leveldb/hlsm_param.h
Collection of most tunable parameters and global variables (e.g. hLSM-tree helper thread handler) in hLSM-tree instance. Other parameters used for db generation can be found in db\_bench.cc and db\_gen.cc.

### include/leveldb/hlsm_type.h
hLSM-tree related classes.

### include/leveldb/hlsm_util.h
Contains class and utility functions related to YCSB key generation.

### table/block_builder.(cc|h)
Modified to allow the same write buffer to be used by both primary and secondary storage under a mirrored scenario. Hence the buffer will be freed or reset only after the content has been written to both storages.

### table/table.cc
The file is adapted to support unified caching for mirrored table.

### util/env_posix.cc
Adds support for mirrored file.

### util/hlsm_util.cc
(1) helper thread to perform I/O on secondary storage. It takes requests from two queues. The high priority queue involves read requests and pre-fetching requests that need to be served ASAP; the low priority queue contains compaction write requests and other posix I/O operations. (2) PosixBufferFile class implements a file class that first buffers all write content, then flush them to storage with direct I/O. The class is dedicated to compaction write on HDD storage. (3) FullMirror_PosixWritableFile class implements the mirrored file. It uses the helper thread to avoid I/O blocking upon HDD storage. (4) implementation of YCSBKeyGenerator.

## Cursor Compaction
cursor re-organizes the compaction. It separates each level into two parts, left and right, which
contains files without overlapped key ranges. L0.L is unchanged, which implies that files inside may overlap each other.
```
 		L0.L|L0.R
 		L1.L|L1.R
 		L2.L|L2.R
 		...
```

 For each compaction, a file is picked from LX.L and merged to
 	the L(X+1). The maximum size of L1.L and L1.R can still be calculated
 	by MaxBytesForLevel.

 In our implementation, we treat LX.L and LX.R as two adjacent levels,
 	L(2X) and L(2X-1). Then we modified the compaction and ratio -related procedures
 	to enforce the control of cursor.

 When LX.R is full, we expect LX.L empty. If so, we can then rename LX.R as LX.L, and set LX.R empty.

## hLSM-tree Level Conversion

|primary storage   |secondary storage    |logical level	|    physical level on primary  |physical level on secondary|
| ------------ | --------------- | -------- | ----------------- | ----------- |
|L0.L\|L0.R    |L0.L\|L0.R       |LL0       |L1\|L0             |L1\|L0       |
|L1.L\|L1.R    |L1.NEW\|…\|R2\|R1|LL1       |L3\|L2             |L?\|…\|L3\|L2|
|...... | | | | |
|X.L\|X.R      |…\|R2\|R1        |LLX       |(end of 2-phase level)| |
|X+1.L\|X+1.R  |X+1.L\|X+1.R     |LL\<X+1\>   | | |
 
Note that we mirrored L0.L and L0.R. 
Last 2-phase logical level (set by hlsm::runtime::two\_phase\_end\_level) has no LX.NEW level. Last level (or set hlsm::runtime::mirror\_start\_level) is fully mirrored. The LX.NEW on secondary indicates
that the level has been lazily copied from the primary; R1, R2, ... stand for the delta levels;
The physical level indicates how the levels are really organized within each Version instance.
