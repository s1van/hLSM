#include <algorithm>

#include "leveldb/status.h"
#include "leveldb/hlsm.h"
#include "leveldb/table.h"
#include "leveldb/options.h"
#include "leveldb/env.h"

#include "port/port_posix.h"
#include "table/filter_block.h"
#include "table/format.h"

#include "db/memtable.h"
#include "db/builder.h"
#include "db/version_set.h"
#include "db/lazy_version_set.h"
#include "db/version_edit.h"
#include "db/lazy_version_edit.h"

#include "db/table_cache.h"
#include "db/db_impl.h"
#include "db/filename.h"



namespace leveldb{

Status TableCache::PreLoadTable(uint64_t file_number, uint64_t file_size) {
	Cache::Handle* handle = NULL;
	Status s = FindTable(file_number, file_size, &handle);
	DEBUG_INFO(3, "file: %lu, size: %lu\n", file_number, file_size);
	// assert(handle != NULL);
	if(handle != NULL) cache_->Release(handle);

	return s;
}

int Version::PreloadMetadata(int max_level, bool update_table_level) {
	DEBUG_INFO(1, "Start\n");
	for (int level = 0; level < max_level; level++) {
		size_t num_files = files_[level].size();
		FileMetaData* const* files = &files_[level][0];
		DEBUG_INFO(1, "Level %d:\t", level);
		for (uint32_t i = 0; i < num_files; i++) {
			if (update_table_level) {
				hlsm::runtime::table_level.add(files[i]->number, level);
				DEBUG_PRINT(1, "%lu[%d]\t", files[i]->number, hlsm::runtime::table_level.get(files[i]->number));
			}
			DEBUG_INFO(3, "file %lu is in use (%d)\n", files[i]->number,
					hlsm::runtime::FileNameHash::inuse(files[i]->number));
			Status s = vset_->table_cache_->PreLoadTable(files[i]->number, files[i]->file_size);
		}
		DEBUG_PRINT(1, "\n");
	}
	DEBUG_INFO(1, "End\n");
	return 0;
}

/*
 * Extend VersionSet
 */

Status BasicVersionSet::MoveLevelDown(Compaction* c, port::Mutex *mutex_) {
    assert(c->num_input_files(1) == 0);
    int level = c->level();
    leveldb::FileMetaData* const* files = &this->current()->files_[level][0];
    size_t num_files = this->current()->files_[level].size();
    DEBUG_INFO(2, "move %lu files from level %d to level %d\n", num_files, level, level+1);

    for(int i = 0; i < num_files; i++) {
    	leveldb::FileMetaData* f = files[i];
    	c->edit()->DeleteFile(level, f->number);
    	c->edit()->AddFile(level + 1, f->number, f->file_size,
    	                       f->smallest, f->largest);
    	hlsm::runtime::table_level.add(f->number, c->level()+1);
    	DEBUG_INFO(3, "[%d/%lu] number: %lu\t size: %lu\n", i+1, num_files, f->number, f->file_size);
    }

    if (c->level() + 1 == hlsm::runtime::mirror_start_level) // need to copy the content to secondary
    	for(int i = 0; i < num_files; i++) {
    		leveldb::FileMetaData* f = files[i];
    		OPQ_ADD_COPYFILE(hlsm::runtime::op_queue,
    			new std::string(leveldb::TableFileName(hlsm::config::primary_storage_path, f->number)), f->number );
    	}

    leveldb::Status status = this->LogAndApply(c->edit(), mutex_);
    return status;
}

Status BasicVersionSet::MoveFileDown(Compaction* c, port::Mutex *mutex_) {
	assert(c->num_input_files(0) == 1);
	FileMetaData* f = c->input(0, 0);
	int level = c->level();
	VersionEdit *edit = c->edit();

	DEBUG_INFO(2, "number: %lu\tlevel: %d\n", f->number, level);
	edit->DeleteFile(level, f->number);
	edit->AddFile(level + 1, f->number, f->file_size,
			f->smallest, f->largest);
	hlsm::runtime::table_level.add(f->number, level+1);
	if (level + 1 == hlsm::runtime::mirror_start_level) { // need to copy the content to secondary
		OPQ_ADD_COPYFILE(hlsm::runtime::op_queue,
				new std::string(TableFileName(hlsm::config::primary_storage_path, f->number)), f->number);
	}
	leveldb::Status status = this->LogAndApply(c->edit(), mutex_);

	return status;
}

VersionEdit* NewVersionEdit (VersionSet* v) {
	DEBUG_INFO(2, "VersionSet: %p\n", v);
	if (hlsm::config::mode.ishLSM() ) {
		if (v != NULL)
			return new LazyVersionEdit(v); // set the delta level offsets
		else
			return new LazyVersionEdit();
	}
	return new BasicVersionEdit();
}

VersionSet *NewVersionSet(const std::string& dbname, const Options* options,
        TableCache* table_cache, const InternalKeyComparator* cmp) {
	DEBUG_INFO(1, "cmp = %p, %s\n", cmp, cmp->Name());
	if (hlsm::config::mode.ishLSM() )
		return new LazyVersionSet(dbname, options, table_cache, cmp);
	return new BasicVersionSet(dbname, options, table_cache, cmp);
}

/*
 * Table extension
 */

struct Table::Rep {
  Options options;
  Status status;
  uint64_t cache_id;
  FilterBlockReader* filter;
  const char* filter_data;

  RandomAccessFile* primary_;
  RandomAccessFile* secondary_;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
};

RandomAccessFile* Table::PickFileHandler(Table::Rep* rep, bool is_sequential) {
	assert(rep->primary_ != NULL || rep->secondary_ != NULL);
	RandomAccessFile* ret = NULL;
	Env* env = rep->options.env;
	if(hlsm::read_from_primary(is_sequential)) {
		if (rep->primary_ == NULL) {
			std::string pname = SECONDARY_TO_PRIMARY_FILE(rep->secondary_->GetFileName());
			if (env->FileExists(pname)) {
				Status s = env->NewRandomAccessFile(pname, &(rep->primary_));
				if (!s.ok()) {
					DEBUG_INFO(2, "File %s exists, but can not be opened, %s\n", 
						pname.c_str(), s.ToString().c_str());
					rep->primary_ = NULL;
				} else {
					// DEBUG_INFO(2, "%p, %s\n", rep->primary_, rep->primary_->GetFileName().c_str());
				}

			}
		}
		ret = (rep->primary_ != NULL)? rep->primary_ : rep->secondary_;
	} else {
		if (hlsm::config::secondary_storage_path != NULL) {
			if (rep->secondary_ == NULL) {
				std::string sname = PRIMARY_TO_SECONDARY_FILE(rep->primary_->GetFileName());
				if (!hlsm::runtime::FileNameHash::inuse(sname) ) {
					DEBUG_INFO(2, "%p, %s\n", rep->primary_, rep->primary_->GetFileName().c_str());
					if (env->FileExists(sname)) {
						Status s = env->NewRandomAccessFile(sname, &(rep->secondary_));
						if (!s.ok()) {
							DEBUG_INFO(2, "File %s exists, but can not be opened, %s\n", 
							sname.c_str(), s.ToString().c_str());
							rep->secondary_ = NULL;
						}

					}
				}
			}
		}
		ret = (rep->secondary_ != NULL)? rep->secondary_ : rep->primary_;
	}

	if (rep->secondary_ != NULL && ret == rep->secondary_) {
		if (is_sequential && hlsm::runtime::seqential_read_from_primary && rep->primary_ != NULL)
                	ret = rep->primary_;
	}

	DEBUG_INFO(3, "ret = %p, primary = %p, secondary = %p, file: %s\n",
			ret, rep->primary_, rep->secondary_, ret->GetFileName().c_str());
	assert(ret != NULL);
	return ret;
}

RandomAccessFile* Table::PickFileHandler(bool is_sequential) {
	return Table::PickFileHandler(rep_, is_sequential);
}



int Table::PrefetchTable(leveldb::RandomAccessFile* file, uint64_t size) {
	leveldb::Slice buffer_input;
	char *buffer_space = (char*)malloc(size);
	leveldb::Status s = file->Read(0, size, &buffer_input, buffer_space);
	free(buffer_space);
	return 0;
}

/*
 * Tailor DMImpl functions for hLSM-tree
 */

void DBImpl::DeleteObsoleteFiles() {
  if (hlsm::config::mode.ishLSM())
	  DBImpl::HLSMDeleteObsoleteFiles();
  else
	  DBImpl::BasicDeleteObsoleteFiles();
}

void DBImpl::HLSMDeleteObsoleteFiles() {
  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::set<uint64_t> lazy_live = hlsm::runtime::moving_tables_;
  std::set<uint64_t> on_the_fly = hlsm::runtime::moving_tables_;
  (reinterpret_cast<LazyVersionSet*>(versions_))->AddLiveLazyFiles(&lazy_live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        if (type == kTableFile && lazy_live.find(number) == lazy_live.end()) {
          table_cache_->Evict(number);
        }
        // if the file is in use, delete it next time
        if (on_the_fly.find(number) == on_the_fly.end() ) {
        	Log(options_.info_log, "Delete type=%d #%lld\n",
        			int(type), static_cast<unsigned long long>(number));

        	if (lazy_live.find(number) != lazy_live.end()) {
          	// implies that same file appears in both mirrored level and non-mirrored level
        		hlsm::runtime::delete_primary_only = true;
        	}
        	env_->DeleteFile(dbname_ + "/" + filenames[i]);
        	hlsm::runtime::delete_primary_only = false;
        }
      }
    }
  }

  env_->GetChildren(std::string(hlsm::config::secondary_storage_path), &filenames); // Ignoring errors on purpose
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kTableFile:
          keep = (lazy_live.find(number) != lazy_live.end());
          break;
      }

      if (!keep) {
        if (type == kTableFile && live.find(number) == live.end()) { // also not mirrored
          table_cache_->Evict(number);

          Log(options_.info_log, "Delete on Secondary type=%d #%lld\n",
        		  int(type), static_cast<unsigned long long>(number));
          env_->DeleteFile(std::string(hlsm::config::secondary_storage_path) + "/" + filenames[i]);
        }
      }
    }
  }
}

Status DBImpl::WriteLevel0TableToLevel(MemTable* mem, VersionEdit* edit,
                                Version* base, int level) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started written to level %d",
      (unsigned long long) meta.number, level);
  DEBUG_INFO(3, "Write to level %d [start]\n", level);

  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

  DEBUG_INFO(3, "Write to level %d [table built]\n", level);

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long) meta.number,
      (unsigned long long) meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);


  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    edit->AddFile(level, meta.number, meta.file_size,
                  meta.smallest, meta.largest);
    CALL_IF_HLSM(reinterpret_cast<LazyVersionEdit*>(edit)
    	->AddLazyFileByRawLevel(level, meta.number, meta.file_size, meta.smallest, meta.largest,
    			reinterpret_cast<LazyVersionSet*>(versions_)->current_lazy() ));
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

int DBImpl::MaybeCompactMemTableToLevel(int level) {
	if (imm_ == NULL)
	  	return 0;
	mutex_.AssertHeld();
	mutex_.Lock();

  // Save the contents of the memtable as a new Table
  VersionEdit &edit = (*NewVersionEdit(versions_));
  Version* base = versions_->current();
  base->Ref();
  Version* current_lazy = NULL;
  CALL_IF_HLSM(current_lazy = reinterpret_cast<LazyVersionSet*>(versions_)->current_lazy());

  CALL_IF_HLSM(current_lazy->Ref());
  Status s = WriteLevel0TableToLevel(imm_, &edit, base, level);
  base->Unref();
  CALL_IF_HLSM(current_lazy->Unref());

  DEBUG_INFO(1, "To level %d\n", level);

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed

    s = versions_->LogAndApply(&edit, &mutex_);
  }

  delete &edit;
  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = NULL;
    has_imm_.Release_Store(NULL);
    DeleteObsoleteFiles();

    // delete leftover kv-pairs to avoid key range overlapping
    mem_->Unref();
    mem_ = new MemTable(internal_comparator_);
    mem_->Ref();
    mutex_.Unlock();
    return 1;
  } else {
    RecordBackgroundError(s);
  }

  mutex_.Unlock();
  return 0;
}

int DBImpl::AdvanceHLSMActiveDeltaLevel(int level) {
	if(!hlsm::config::mode.ishLSM())
			return 0;

	mutex_.AssertHeld();
	mutex_.Lock();

  VersionEdit* edit = NewVersionEdit(versions_);
  Version* base = versions_->current();
  base->Ref();
  Version* current_lazy = NULL;
  current_lazy = reinterpret_cast<LazyVersionSet*>(versions_)->current_lazy();

  current_lazy->Ref();

  int ok = reinterpret_cast<LazyVersionEdit*>(edit)->AdvanceActiveDeltaLevelByRawLevel(level);
  base->Unref();
  current_lazy->Unref();

  DEBUG_INFO(1, "Advance delta level of level %d\n", level);


  // Replace immutable memtable with the generated Table
  if (ok) {
    edit->SetPrevLogNumber(0);
    edit->SetLogNumber(logfile_number_);  // Earlier logs no longer needed

    versions_->LogAndApply(edit, &mutex_);
  }

  delete edit;
  mutex_.Unlock();
  return 0;
}

} // namespace leveldb

namespace hlsm{
namespace runtime {

int preload_metadata(leveldb::VersionSet* versions) {
	CALL_IF_HLSM(reinterpret_cast<leveldb::LazyVersionSet*>(versions)->current_lazy()
			->PreloadMetadata(hlsm::runtime::kNumLazyLevels, false) );
	versions->current()->PreloadMetadata(leveldb::config::kNumLevels);

	return 0;
}


int init(leveldb::Env* env_) {
	if (hlsm::config::debug_file != NULL)
		debug_fd = fopen(hlsm::config::debug_file, "w");
	DEBUG_INFO(1, "debug_fd = %p\tstderr = %p\n", debug_fd, stderr);

	hlsm::runtime::env_ = env_;
	if (hlsm::config::mode.isDefault()) {
		leveldb::config::kMaxMemCompactLevel = 0;
		use_cursor_compaction = false;
		hlsm::config::secondary_storage_path = NULL;

	} else if (hlsm::config::mode.isFullMirror()) {
		full_mirror = true;
		mirror_start_level = 0;
		seqential_read_from_primary = false; // primary is SSD, secondary is HDD
		random_read_from_primary = true;
		meta_on_primary = true;
		log_on_primary = true;
		use_opq_thread = true;
		leveldb::config::kMaxMemCompactLevel = 0;
		hlsm::config::append_by_opq = false;
		if (hlsm::config::secondary_storage_path == NULL) {
			fprintf(stderr, "secondary_storage_path is NULL\n");
			exit(0);
		}

	} else if (hlsm::config::mode.isPartialMirror()) {
		full_mirror = false;
		mirror_start_level = 3;
		seqential_read_from_primary = true; // primary is HDD, secondary is SSD
		random_read_from_primary = true;
		meta_on_primary = false;
		log_on_primary = false;
		use_opq_thread = true;
		if (hlsm::config::secondary_storage_path == NULL) {
			fprintf(stderr, "secondary_storage_path is NULL\n");
			exit(0);
		}

	} else if (hlsm::config::mode.isbLSM()) {
		full_mirror = false;
		use_cursor_compaction = true;
		meta_on_primary = true;
		log_on_primary = true;
		leveldb::config::kMaxMemCompactLevel = 0;

	} else if (hlsm::config::mode.isPartialbLSM()) {
		full_mirror = false;
		mirror_start_level = 3;
		seqential_read_from_primary = true; // primary is HDD, secondary is SSD
		random_read_from_primary = true;
		use_cursor_compaction = true;
		meta_on_primary = false;
		log_on_primary = false;
		use_opq_thread = true;
		leveldb::config::kMaxMemCompactLevel = 0;

	} else if (hlsm::config::mode.ishLSM()) {
		full_mirror = false;
		top_mirror_end_level = 1; // physical level on primary storage; level starts at 0
		top_pure_mirror_end_level = 0; // physical level on primary storage
		use_cursor_compaction = true;
		seqential_read_from_primary = true; // primary is HDD, secondary is SSD
		random_read_from_primary = false;
		meta_on_primary = false;
		log_on_primary = false;
		use_opq_thread = true;
		two_phase_end_level = 6; // cursor (logical) level; level starts at 0
		mirror_start_level = two_phase_end_level * 2; // physical level on primary storage
		leveldb::config::kMaxMemCompactLevel = 0; // do not write memtable to levels other than 0
		if (hlsm::config::secondary_storage_path == NULL) {
			fprintf(stderr, "secondary_storage_path is NULL\n");
			exit(0);
		}
	} else {
		fprintf(stderr, "unknown mode\n");
		exit(0);
	}

	if (use_opq_thread)
		hlsm::init_opq_helpler();

	runtime::kMinBytesPerSeek = 1; //config::kMinKBPerSeek * 1024;

	return 0;
}

int cleanup() {
	DEBUG_INFO(1, "debug_fd = %p\tstderr = %p\n", debug_fd, stderr);
	if (debug_fd != stderr)
		fclose(debug_fd);
	return 0;
}

} // runtime


/*
 * hlsm_type.h
 */

bool DeltaLevelMeta::is_valid_detla_meta() {
	return start <= hlsm::runtime::delta_level_num &&
		   clear <= hlsm::runtime::delta_level_num &&
		   active <= hlsm::runtime::delta_level_num;
}

int TableLevel::get(uint64_t key){
	mutex_.Lock();
	int value;
	if (mapping_.find(key) == mapping_.end())
		value = -1;
	else
		value = mapping_.find(key)->second;
	mutex_.Unlock();
	return value;
}

uint64_t TableLevel::getLatest() {
	return latest;
}

int TableLevel::add(uint64_t key, int raw_level){
	DEBUG_INFO(3, "level: %d\tfile number: %lu\n", raw_level, key);
	mutex_.Lock();
	mapping_[key] = raw_level;
	latest = key;
	mutex_.Unlock();
	return 0;
}

int TableLevel::remove(uint64_t key){
	mutex_.Lock();
	DEBUG_INFO(2, "file number: %lu\n", key);
	mapping_.erase(key);
	mutex_.Unlock();
	return 0;
}

bool TableLevel::withinMirroredLevel(uint64_t key){
	int level = get(key);
	if (level == -1) return false;
	DEBUG_INFO(2, "file number: %lu\tlevel: %d\n", key, level);
	return (level >= runtime::mirror_start_level ||
		level <= runtime::top_mirror_end_level); // <=1 for hLSM-tree 2-phase compaction
}

bool TableLevel::withinPureMirroredLevel(uint64_t key){
	int level = get(key);
	if (level == -1) return false;
	DEBUG_INFO(2, "file number: %lu\tlevel: %d\n", key, level);
	return (level >= runtime::mirror_start_level ||
			level <= runtime::top_pure_mirror_end_level); // delete obsolete level 0 file on secondary
}

/*
 * hlsm_impl.h
 */

int delete_secondary_table(leveldb::Env* const env, uint64_t number) {
	if (hlsm::config::secondary_storage_path == NULL)
		return 0;
	std::string fname = leveldb::TableFileName(hlsm::config::secondary_storage_path, number);
	if (env->FileExists(fname))
		env->DeleteFile(fname);
	return 0;
}

// proper = true implies getting the proper file
std::string get_table_path(uint64_t number, bool is_seq, bool proper) {
	bool from_primary = read_from_primary(is_seq);
	if ( (from_primary && proper) || (!from_primary && !proper) || hlsm::runtime::FileNameHash::inuse(number))
		return leveldb::TableFileName(hlsm::config::primary_storage_path, number);
	else
		return leveldb::TableFileName(hlsm::config::secondary_storage_path, number);
}


} // hlsm
