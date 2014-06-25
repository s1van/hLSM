#include <algorithm>
#include <stdio.h>
#include <bits/algorithmfwd.h>

#include "db/version_set.h"
#include "db/lazy_version_set.h"
#include "db/lazy_version_edit.h"
#include "db/filename.h"
#include "db/hlsm_impl.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "leveldb/hlsm.h"

namespace leveldb {

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
class LazyVersionSet::Builder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        return (f1->number < f2->number);
      }
    }
  };

  typedef std::set<FileMetaData*, BySmallestKey> FileSet;
  struct LevelState {
    std::set<uint64_t> deleted_files;
    FileSet* added_files;
  };

  VersionSet* vset_;
  Version* base_;
  Version* lazy_base_;
  LevelState levels_[leveldb::config::kNumLevels];
  LevelState lazy_levels_[hlsm::runtime::kNumLazyLevels];

 public:
  // Initialize a builder with the files from *base and other info from *vset
  Builder(VersionSet* vset, Version* base, Version* lazy_base)
      : vset_(vset),
        base_(base),
        lazy_base_(lazy_base) {
    base_->Ref();
    lazy_base_->Ref();
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < leveldb::config::kNumLevels; level++) {
      levels_[level].added_files = new FileSet(cmp);
    }
    for (int level = 0; level < hlsm::runtime::kNumLazyLevels; level++) {
      lazy_levels_[level].added_files = new FileSet(cmp);
    }
  }

  inline static void FreeLevels(LevelState *levels, int num) {
	  for (int level = 0; level < num; level++) {
		  const FileSet* added = levels[level].added_files;
		  std::vector<FileMetaData*> to_unref;
		  to_unref.reserve(added->size());
		  for (FileSet::const_iterator it = added->begin();
				  it != added->end(); ++it) {
			  to_unref.push_back(*it);
		  }
		  delete added;
		  for (uint32_t i = 0; i < to_unref.size(); i++) {
			  FileMetaData* f = to_unref[i];
			  f->refs--;
			  if (f->refs <= 0) {
				  delete f;
			  }
		  }
	  }
	  return;
  }

  ~Builder() {
	FreeLevels(levels_, leveldb::config::kNumLevels);
	FreeLevels(lazy_levels_, hlsm::runtime::kNumLazyLevels);
    base_->Unref();
    lazy_base_->Unref();
  }

  // Apply all of the edits in *edit to the current state.
  void Apply(LazyVersionEdit* edit) {
    // Update compaction pointers
    for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
      const int level = edit->compact_pointers_[i].first;
      vset_->compact_pointer_[level] =
          edit->compact_pointers_[i].second.Encode().ToString();
    }

    // Delete files
    const LazyVersionEdit::DeletedFileSet& del = edit->deleted_files_;
    for (LazyVersionEdit::DeletedFileSet::const_iterator iter = del.begin();
         iter != del.end();
         ++iter) {
      const int level = iter->first;
      const uint64_t number = iter->second;
      levels_[level].deleted_files.insert(number);
    }

    const LazyVersionEdit::DeletedFileSet& ldel = edit->deleted_files_lazy_;
    if (ldel.size() > 0) {
    	for (LazyVersionEdit::DeletedFileSet::const_iterator iter = ldel.begin();
    			iter != ldel.end();
    			++iter) {
    		const int level = iter->first;
    		const uint64_t number = iter->second;
    		lazy_levels_[level].deleted_files.insert(number);
    	}
    }

    // Add new files
    for (size_t i = 0; i < edit->new_files_.size(); i++) {
      const int level = edit->new_files_[i].first;
      FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
      f->refs = 1;

      // We arrange to automatically compact this file after
      // a certain number of seeks.  Let's assume:
      //   (1) One seek costs 10ms
      //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      //   (3) A compaction of 1MB does 25MB of IO:
      //         1MB read from this level
      //         10-12MB read from next level (boundaries may be misaligned)
      //         10-12MB written to next level
      // This implies that 25 seeks cost the same as the compaction
      // of 1MB of data.  I.e., one seek costs approximately the
      // same as the compaction of 40KB of data.  We are a little
      // conservative and allow approximately one seek for every 16KB
      // of data before triggering a compaction.
      f->allowed_seeks = (f->file_size / hlsm::runtime::kMinBytesPerSeek);
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;

      levels_[level].deleted_files.erase(f->number);
      levels_[level].added_files->insert(f);
    }

    for (size_t i = 0; i < edit->new_files_lazy_.size(); i++) {
      const int level = edit->new_files_lazy_[i].first;
      FileMetaData* f = new FileMetaData(edit->new_files_lazy_[i].second);
      f->refs = 1;

      f->allowed_seeks = (f->file_size / hlsm::runtime::kMinBytesPerSeek);
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;

      lazy_levels_[level].deleted_files.erase(f->number);
      lazy_levels_[level].added_files->insert(f);
    }
  }

  inline static void MaybeAddFile(Version* v, LevelState levels[], int level, FileMetaData* f) {
    if (levels[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
    } else {
      std::vector<FileMetaData*>* files = &v->files_[level];
      if (level > 0 && !files->empty()) {
        // Must not overlap
        assert(vset_->icmp_.Compare((*files)[files->size()-1]->largest,
                                    f->smallest) < 0);
      }
      f->refs++;
      files->push_back(f);
    }
  }

  // Save the current state in *v.
  inline void SaveTo(Version* v, Version *base, LevelState levels[], int kLevel) {
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < kLevel; level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const std::vector<FileMetaData*>& base_files = base->files_[level];
      std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
      const FileSet* added = levels[level].added_files;
      v->files_[level].reserve(base_files.size() + added->size());
      for (FileSet::const_iterator added_iter = added->begin();
           added_iter != added->end();
           ++added_iter) {
        // Add all smaller files listed in base_
        for (std::vector<FileMetaData*>::const_iterator bpos
                 = std::upper_bound(base_iter, base_end, *added_iter, cmp);
             base_iter != bpos;
             ++base_iter) {
          MaybeAddFile(v, levels, level, *base_iter);
        }

        MaybeAddFile(v, levels, level, *added_iter);
      }

      // Add remaining base files
      for (; base_iter != base_end; ++base_iter) {
    	MaybeAddFile(v, levels, level, *base_iter);
      }

#ifndef NDEBUG
      // Make sure there is no overlap in levels > 0
      if (level > 0) {
        for (uint32_t i = 1; i < v->files_[level].size(); i++) {
          const InternalKey& prev_end = v->files_[level][i-1]->largest;
          const InternalKey& this_begin = v->files_[level][i]->smallest;
          if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
            fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                    prev_end.DebugString().c_str(),
                    this_begin.DebugString().c_str());
            abort();
          }
        }
      }
#endif
    }
  }

  void SaveTo(Version* v, Version* lv) {
	  SaveTo(v, base_, levels_, leveldb::config::kNumLevels);
	  SaveTo(lv, lazy_base_, lazy_levels_, hlsm::runtime::kNumLazyLevels);
  }

};

LazyVersionSet::LazyVersionSet(const std::string& dbname,
                       const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp)
    :VersionSet(dbname, options, table_cache, cmp),
     dummy_lazy_versions_(this, hlsm::runtime::kNumLazyLevels),
     current_lazy_(NULL) {
  AppendVersion(new Version(this), new Version(this, hlsm::runtime::kNumLazyLevels) );
}

LazyVersionSet::~LazyVersionSet() {
  current_->Unref();
  current_lazy_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  assert(dummy_lazy_versions_.next_ == &dummy_lazy_versions_);  // List must be empty
  delete descriptor_log_;
  delete descriptor_file_;
}

void LazyVersionSet::AppendVersion(Version* v, Version* lv) {
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != NULL) {
    current_->Unref();
  }
  current_ = v;
  v->Ref();

  // Append to linked list
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;

  // Make "lv" current
  assert(lv->refs_ == 0);
  assert(lv != current_lazy_);
  if (current_lazy_ != NULL) {
	  current_lazy_->Unref();
  }
  current_lazy_ = lv;
  lv->Ref();

  // Append to linked list
  lv->prev_ = dummy_lazy_versions_.prev_;
  lv->next_ = &dummy_lazy_versions_;
  lv->prev_->next_ = lv;
  lv->next_->prev_ = lv;
}

Status LazyVersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {
  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_);
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);
  }

  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);

  Version* v = new Version(this);
  Version* lv = new Version(this, hlsm::runtime::kNumLazyLevels);
  {
	Builder builder(this, current_, current_lazy_);
    builder.Apply(reinterpret_cast<LazyVersionEdit*>(edit) );
    builder.SaveTo(v, lv);
  }
  Finalize(v); // calculate scores for each level

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  std::string new_manifest_file;
  Status s;
  if (descriptor_log_ == NULL) {
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    assert(descriptor_file_ == NULL);
    new_manifest_file = leveldb::DescriptorFileName(dbname_, manifest_file_number_);
    edit->SetNextFile(next_file_number_);
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
    if (s.ok()) {
      descriptor_log_ = new log::Writer(descriptor_file_);
      s = WriteSnapshot(descriptor_log_);
    }
  }

  // Unlock during expensive MANIFEST log write
  {
    mu->Unlock();

    // Write new record to MANIFEST log
    if (s.ok()) {
      std::string record;
      edit->EncodeTo(&record);
      s = descriptor_log_->AddRecord(record);
      if (s.ok()) {
        s = descriptor_file_->Sync();
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }

    mu->Lock();
  }

  // Install the new version
  if (s.ok()) {
    AppendVersion(v, lv);
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;
  } else {
    delete v;
    if (!new_manifest_file.empty()) {
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = NULL;
      descriptor_file_ = NULL;
      env_->DeleteFile(new_manifest_file);
    }
  }

  DEBUG_LEVEL_CHECK(2, PrintVersionSet());

  return s;
}

Status LazyVersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata
  LazyVersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save compaction pointers
  for (int level = 0; level < leveldb::config::kNumLevels; level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      edit.SetCompactPointer(level, key);
    }
  }

  // Save files
  for (int level = 0; level < leveldb::config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
    }
  }

  for (int level = 0; level < hlsm::runtime::kNumLazyLevels; level++) {
    const std::vector<FileMetaData*>& files = current_lazy_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      edit.AddLazyFile(level, f->number, f->file_size, f->smallest, f->largest);
    }
  }

  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}


Status LazyVersionSet::Recover() {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    virtual void Corruption(size_t bytes, const Status& s) {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size()-1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  std::string dscname = dbname_ + "/" + current;
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  Builder builder(this, current_, current_lazy_);

  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true/*checksum*/, 0/*initial_offset*/);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      LazyVersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      if (s.ok()) {
        builder.Apply(&edit);
      }

      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
    }
  }
  delete file;
  file = NULL;

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    Version* v = new Version(this);
    Version* lv = new Version(this, hlsm::runtime::kNumLazyLevels);
    builder.SaveTo(v, lv);
    // Install recovered version
    Finalize(v);
    AppendVersion(v, lv);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;
  }

  return s;
}

void LazyVersionSet::AddLiveLazyFiles(std::set<uint64_t>* live) {
  for (Version* v = dummy_lazy_versions_.next_;
       v != &dummy_lazy_versions_;
       v = v->next_) {
    for (int level = 0; level < hlsm::runtime::kNumLazyLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        live->insert(files[i]->number);
      }
    }
  }
}

Status LazyVersionSet::MoveFileDown(Compaction* c, port::Mutex *mutex_) {
	assert(c->num_input_files(0) == 1);
	FileMetaData* f = c->input(0, 0);
	int level = c->level();
	LazyVersionEdit* edit = reinterpret_cast<LazyVersionEdit*>(c->edit());

	DEBUG_INFO(2, "number: %lu\tlevel: %d\n", f->number, level);
	edit->DeleteFile(level, f->number);
	edit->AddFile(level + 1, f->number, f->file_size,
			f->smallest, f->largest);
	hlsm::runtime::table_level.add(f->number, level+1);
	if (level + 1 == hlsm::runtime::mirror_start_level) { // need to copy the content to secondary
		OPQ_ADD_COPYFILE(hlsm::runtime::op_queue,
			new std::string(TableFileName(hlsm::config::primary_storage_path, f->number)));
	}
	leveldb::Status status = this->LogAndApply(c->edit(), mutex_);

	return status;
}

Status LazyVersionSet::MoveLevelDown(Compaction* c, port::Mutex *mutex_){
	//ToDo: handle lazy levels
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

Compaction* LazyVersionSet::PickCompaction() {
  Compaction* c;
  int level;

  // We prefer compactions triggered by too much data in a level over
  // the compactions triggered by seeks.
  const bool size_compaction = (current_->compaction_score_ >= 1);
  const bool seek_compaction = (current_->file_to_compact_ != NULL);
  if (size_compaction) {
    level = current_->compaction_level_;
    assert(level >= 0);
    assert(level+1 < config::kNumLevels);
    c = new Compaction(level);

    // Pick the first file that comes after compact_pointer_[level]
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      FileMetaData* f = current_->files_[level][i];
      if (compact_pointer_[level].empty() ||
          icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
        c->inputs_[0].push_back(f);
        break;
      }
    }
    if (c->inputs_[0].empty()) {
      // Wrap-around to the beginning of the key space
      c->inputs_[0].push_back(current_->files_[level][0]);
    }
  } else if (seek_compaction) {
    level = current_->file_to_compact_level_;
    c = new Compaction(level);
    c->inputs_[0].push_back(current_->file_to_compact_);
  } else {
    return NULL;
  }

  c->input_version_ = current_;
  c->input_version_->Ref();

  // Pick up all files for level 0
  if (level == 0) {
	c->inputs_[0].clear();
	for (size_t i = 0; i < current_->files_[level].size(); i++) {
		FileMetaData* f = current_->files_[level][i];
		c->inputs_[0].push_back(f);
	}
    assert(!c->inputs_[0].empty());
  }

  SetupOtherInputs(c);

  return c;
}

void LazyVersionSet::PrintVersionSet() {
	DEBUG_PRINT(0, "Print Version:\n");
	for (Version* v = dummy_versions_.next_;
			v != &dummy_versions_;
			v = v->next_) {
		for (int level = 0; level < config::kNumLevels; level++) {
			const std::vector<FileMetaData*>& files = v->files_[level];
			if (files.size() > 0) {
				DEBUG_PRINT(0, "[level %d]\t", level);
				for (size_t i = 0; i < files.size(); i++) {
					DEBUG_PRINT(0, "%lu\t", files[i]->number);
				}
				DEBUG_PRINT(0, "\n");
			}
		}
	}

	DEBUG_PRINT(0, "Print Lazy Version:\n");
	for (Version* v = dummy_lazy_versions_.next_;
			v != &dummy_lazy_versions_;
			v = v->next_) {
		for (int level = 0; level < hlsm::runtime::kNumLazyLevels; level++) {
			const std::vector<FileMetaData*>& files = v->files_[level];
			if (files.size() > 0) {
				DEBUG_PRINT(0, "[level %d]\t", level);
				for (size_t i = 0; i < files.size(); i++) {
					DEBUG_PRINT(0, "%lu\t", files[i]->number);
				}
				DEBUG_PRINT(0, "\n");
			}
		}
	}
}

} // namespace leveldb
