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

## db_gen
Create by 'make db_gen'.

*db_gen* is used to generate large multi-level db instance without compaction. So the time cost due to compaction-induced write amplification is totally avoided. It has arguments similar to db_bench.
