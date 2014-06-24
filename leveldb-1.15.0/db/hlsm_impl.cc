#include <algorithm>

#include "leveldb/status.h"
#include "leveldb/hlsm.h"
#include "leveldb/table.h"
#include "leveldb/options.h"

#include "port/port_posix.h"
#include "table/filter_block.h"
#include "table/format.h"

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
	cache_->Release(handle);

	return s;
}

int Version::PreloadMetadata(int max_level) {
	DEBUG_INFO(1, "Start\n");
	for (int level = 0; level < std::max(config::kNumLevels, max_level); level++) {
		size_t num_files = files_[level].size();
		FileMetaData* const* files = &files_[level][0];
		DEBUG_INFO(1, "Level %d:\t", level);
		for (uint32_t i = 0; i < num_files; i++) {
			Status s = vset_->table_cache_->PreLoadTable(files[i]->number, files[i]->file_size);
			hlsm::runtime::table_level.add(files[i]->number, level);
			DEBUG_PRINT(1, "%lu[%d]\t", files[i]->number, hlsm::runtime::table_level.get(files[i]->number));
		}
		DEBUG_PRINT(1, "\n");
	}
	DEBUG_INFO(1, "End\n");
	return 0;
}

Status BasicVersionSet::MoveLevelDown(Compaction* c, port::Mutex *mutex_){
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
    			new std::string(leveldb::TableFileName(hlsm::config::primary_storage_path, f->number)) );
    	}

    leveldb::Status status = this->LogAndApply(c->edit(), mutex_);
    return status;
}

VersionEdit* NewVersionEdit () {
	if (hlsm::config::mode.ishLSM() )
		return new LazyVersionEdit();
	return new BasicVersionEdit();
}

VersionSet *NewVersionSet(const std::string& dbname, const Options* options,
        TableCache* table_cache, const InternalKeyComparator* cmp) {
	if (hlsm::config::mode.ishLSM() )
		return new LazyVersionSet(dbname, options, table_cache, cmp);
	return new BasicVersionSet(dbname, options, table_cache, cmp);
}

struct Table::Rep {
  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;
  FilterBlockReader* filter;
  const char* filter_data;

  RandomAccessFile* primary_;
  std::string sfname_;
  RandomAccessFile* secondary_; // file may not exist at the beginning, if so, use sfname to open later
  bool should_read_from_secondary; // useful when is_sequential is not available

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
};

/*
 *  DO:	direct rep_->file to secondary or primary
 * PRE:	rep_ must be initialized in advance
 *  IN:	is_sequential == -1 implies that pattern is unknown
 */
//ToDO: remove the assumption that no file is accessed both randomly and sequentially at the same time
int Table::SetFileDescriptor(int is_sequential) {
	rep_->file = rep_->primary_;
	if (is_sequential == - 1) { // unknown pattern
		if (rep_->should_read_from_secondary)
			if (rep_->secondary_ != NULL) {
				rep_->file = rep_->secondary_;
			} else {
				if (!hlsm::runtime::FileNameHash::inuse(rep_->sfname_)) { // file is written right now
					Status s = rep_->options.env->NewRandomAccessFile(rep_->sfname_, &(rep_->secondary_));
					rep_->file = rep_->secondary_;
				}
			}
	} else {
		if (hlsm::read_from_primary(is_sequential)) {
			rep_->file = rep_->primary_;
			rep_->should_read_from_secondary = false;
		} else if (rep_->secondary_ != NULL) {
			rep_->file = rep_->secondary_;
		} else { // need to open the file
			if (rep_->sfname_ != NULL) {
				if (hlsm::is_mirrored_write(rep_->sfname_)) {
					rep_->should_read_from_secondary = true;
					if (!hlsm::runtime::FileNameHash::inuse(rep_->sfname_)) { // file is written right now
						Status s = rep_->options.env->NewRandomAccessFile(rep_->sfname_, &(rep_->secondary_));
						rep_->file = rep_->secondary_;
					}
				}
			}
		}
	}

	DEBUG_INFO(3, "is_sequential: %d\tsfname: %s\tfile: %p\tprimary: %p\tsecondary:%p\tinuse: %d\tmirrored: %d\n",
			is_sequential, rep_->sfname_.c_str(), rep_->file, rep_->primary_, rep_->secondary_,
			hlsm::runtime::FileNameHash::inuse(rep_->sfname_), hlsm::is_mirrored_write(rep_->sfname_));
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
        Log(options_.info_log, "Delete type=%d #%lld\n",
            int(type),
            static_cast<unsigned long long>(number));
        env_->DeleteFile(dbname_ + "/" + filenames[i]);
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
        		  int(type),
        		  static_cast<unsigned long long>(number));
          env_->DeleteFile(std::string(hlsm::config::secondary_storage_path) + "/" + filenames[i]);
        }
      }
    }
  }
}

} // leveldb

namespace hlsm{
namespace runtime {

int preload_metadata(leveldb::VersionSet* versions) {
	return versions->current()->PreloadMetadata(hlsm::config::preload_metadata_max_level);
}


int init() {
	if (hlsm::config::debug_file != NULL)
		debug_fd = fopen(hlsm::config::debug_file, "w");
	DEBUG_INFO(1, "debug_fd = %p\tstderr = %p\n", debug_fd, stderr);

	if (hlsm::config::mode.isDefault()) {

	} else if (hlsm::config::mode.isFullMirror()) {
		full_mirror = true;
		mirror_start_level = 0;
		seqential_read_from_primary = false; // primary is SSD, secondary is HDD
		random_read_from_primary = true;
		meta_on_primary = true;
		log_on_primary = true;
		use_opq_thread = true;

	} else if (hlsm::config::mode.isPartialMirror()) {
		full_mirror = false;
		mirror_start_level = 3;
		seqential_read_from_primary = true; // primary is HDD, secondary is SSD
		random_read_from_primary = true;
		meta_on_primary = false;
		log_on_primary = false;
		use_opq_thread = true;

	} else if (hlsm::config::mode.isbLSM()) {
		full_mirror = false;
		use_cursor_compaction = true;
		meta_on_primary = true;
		log_on_primary = true;

	} else if (hlsm::config::mode.isPartialbLSM()) {
		full_mirror = false;
		mirror_start_level = 3;
		seqential_read_from_primary = true; // primary is HDD, secondary is SSD
		random_read_from_primary = true;
		use_cursor_compaction = true;
		meta_on_primary = false;
		log_on_primary = false;
		use_opq_thread = true;

	} else if (hlsm::config::mode.ishLSM()) {
		full_mirror = false;
		mirror_start_level = 4; // logical level
		top_mirror_end_level = 1;
		use_cursor_compaction = true;
		seqential_read_from_primary = true; // primary is HDD, secondary is SSD
		random_read_from_primary = true;
		meta_on_primary = false;
		log_on_primary = false;
		use_opq_thread = true;
	}

	if (use_opq_thread)
		hlsm::init_opq_helpler();

	runtime::kMinBytesPerSeek = config::kMinKBPerSeek * 1024;

	return 0;
}

int cleanup() {
	DEBUG_INFO(1, "debug_fd = %p\tstderr = %p\n", debug_fd, stderr);
	if (debug_fd != stderr)
		fclose(debug_fd);
	return 0;
}

} // runtime


int TableLevel::get(uint64_t key){
	if (mapping_.find(key) == mapping_.end())
		return -1;
	else
		return mapping_.find(key)->second;
}

uint64_t TableLevel::getLatest() {
	return latest;
}

int TableLevel::add(uint64_t key, int raw_level){
	DEBUG_INFO(2, "level: %d\tfile number: %lu\n", raw_level, key);
	mapping_[key] = raw_level;
	latest = key;
	return 0;
}

int TableLevel::remove(uint64_t key){
	DEBUG_INFO(2, "file number: %lu\n", key);
	mapping_.erase(key);
	return 0;
}

bool TableLevel::withinMirroredLevel(uint64_t key){
	int level = get(key);
	DEBUG_INFO(2, "file number: %lu\tlevel: %d\n", key, level);
	return (level >= runtime::mirror_start_level ||
		level <= runtime::top_mirror_end_level); // <=1 for hLSM-tree 2-phase compaction
}

bool TableLevel::withinPureMirroredLevel(uint64_t key){
	int level = get(key);
	DEBUG_INFO(2, "file number: %lu\tlevel: %d\n", key, level);
	return (level >= runtime::mirror_start_level ||
			level == std::min(0, runtime::top_mirror_end_level)); // delete obsolete level 0 file on secondary
}

int delete_secondary_file(leveldb::Env* const env, uint64_t number) {
	if (hlsm::config::secondary_storage_path == NULL)
		return 0;
	std::string fname = leveldb::TableFileName(hlsm::config::secondary_storage_path, number);
	if (env->FileExists(fname))
		env->DeleteFile(fname);
	return 0;
}


int prefetch_file(leveldb::RandomAccessFile* file, uint64_t size) {
	leveldb::Slice buffer_input;
	char *buffer_space = (char*)malloc(size);
	leveldb::Status s = file->Read(0, size, &buffer_input, buffer_space);
	free(buffer_space);
	return 0;
}

} // hlsm
