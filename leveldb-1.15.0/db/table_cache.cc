// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "leveldb/hlsm.h"
#include "util/coding.h"

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname,
                       const Options* options,
                       int entries)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {
}

TableCache::~TableCache() {
  delete cache_;
}

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle, bool is_sequential) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));

  DEBUG_INFO(3, "file_number: %lu, sequential? %d\n", file_number, is_sequential);
  *handle = cache_->Lookup(key);
  if (*handle == NULL) {
    std::string fname = hlsm::get_table_path(file_number, is_sequential, true);
    DEBUG_INFO(3, "file_name = %s, is_seq = %d\n", fname.c_str(), is_sequential);

    RandomAccessFile* file = NULL;
    Table* table = NULL;
    s = env_->NewRandomAccessFile(fname, &file);
    DEBUG_INFO(3, "file_name = %s, status = %s\n", fname.c_str(), s.ToString().c_str());
    if (!s.ok()) {
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }

    // maybe the file is on the other store
    if (!s.ok() && hlsm::config::mode.ishLSM() ) {
    	std::string sfname = hlsm::get_table_path(file_number, is_sequential, false);
    	if (env_->NewRandomAccessFile(sfname, &file).ok()) {
    		s = Status::OK();
    	}
    }

    if (s.ok()) {
      DEBUG_INFO(3, "Open %s, %lu\n", file->GetFileName().c_str(), file_number);
      s = Table::Open(*options_, file, file_size, &table, is_sequential);
    }
    DEBUG_INFO(3, "s.ok() = %d, table = %p\n", s.ok(), table);

    if (!s.ok()) {
      assert(table == NULL);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
      assert(*handle != NULL);
    }
  }
  DEBUG_INFO(3, "handle = %p\n", *handle);
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr, bool is_sequential) {
  if (tableptr != NULL) {
    *tableptr = NULL;
  }

  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle, is_sequential);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table;
  table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  DEBUG_INFO(2, "is_sequential = %d, iter_prefetch = %d, raw_prefetch = %d\n",
		  is_sequential, hlsm::config::iterator_prefetch, hlsm::config::raw_prefetch);
  if (hlsm::runtime::use_opq_thread && hlsm::config::iterator_prefetch && is_sequential) {
  	  Cache::Handle* phandle = NULL;
  	  Status s = FindTable(file_number, file_size, &phandle, is_sequential);

	  DEBUG_INFO(2, "Prefetch fnum: %lu, size: %lu\n", file_number, file_size);
	  ReadOptions* opq_options = (ReadOptions*) malloc(sizeof(ReadOptions));
	  opq_options->snapshot = options.snapshot;
	  opq_options->verify_checksums = false;
	  opq_options->fill_cache = true;
	  Iterator* piter = table->NewIterator(*opq_options, is_sequential);
  	  piter->RegisterCleanup(&UnrefEntry, cache_, phandle);

	  OPQ_ADD_ITR_PREFETCH(hlsm::runtime::hop_queue, piter, opq_options);
	  DEBUG_INFO(2, "ITR_PREFETCH op added\n");

  } else if (hlsm::runtime::use_opq_thread && hlsm::config::raw_prefetch && is_sequential) {
	  OPQ_ADD_RAW_PREFETCH(hlsm::runtime::hop_queue,
			  table->PickFileHandler(is_sequential), file_size);
	  DEBUG_INFO(2, "RAW_PREFETCH op added\n");
  }

  Iterator* result = table->NewIterator(options, is_sequential);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);

  if (tableptr != NULL) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&)) {
  Cache::Handle* handle = NULL;
  Status s;
  DEBUG_MEASURE_RECORD(1, (s = FindTable(file_number, file_size, &handle)), "Get--FindTable");
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    DEBUG_MEASURE_RECORD(1, (s = t->InternalGet(options, k, arg, saver, false)), "TableCache::Get--InternatGet");
    cache_->Release(handle);
  }
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
