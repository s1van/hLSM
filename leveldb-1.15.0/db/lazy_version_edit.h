#ifndef HLSM_LAZY_VERSION_EDIT_H_
#define HLSM_LAZY_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/version_edit.h"
#include "leveldb/hlsm.h"

namespace leveldb {

class VersionSet;

class LazyVersionEdit: public leveldb::VersionEdit {
 public:
	LazyVersionEdit();
	LazyVersionEdit(VersionSet*);
    void Clear();

  void AddLazyFile(int level, uint64_t file,
               uint64_t file_size,
               const InternalKey& smallest,
               const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_lazy_.push_back(std::make_pair(level, f));
    DEBUG_INFO(2,"size: %lu fnum: %lu\tlevel: %d\tfp: %p\n", file_size, file, level, &f);
  }

  // Delete the specified "file" from the specified "level".
  void DeleteLazyFile(int level, uint64_t file) {
	deleted_files_lazy_.insert(std::make_pair(level, file));
	DEBUG_INFO(2,"fnum: %lu\tlevel: %d\n", file, level);
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

  void SetDeltaLevels(hlsm::delta_meta_t meta[]) {
	  for (int level = 0; level < hlsm::runtime::kLogicalLevels; level++) {
		  delta_meta_[level].set_delta_meta(&meta[level]);
		  assert(delta_meta_[level].is_valid_detla_meta());
	  }
  }

  void SetDeltaLevels(VersionSet* v);

//  inline int AdvanceActiveDeltaLevel(int level) {
//	  int llevel = hlsm::get_logical_level(level);
//	  //lazy_delta_level_offset_[llevel]++;
//	  DEBUG_INFO(2, "llevel: %d", llevel);
//	  hlsm::debug_detla_meta(&(delta_meta_[llevel]));
//	  assert(hlsm::is_valid_detla_meta( &(delta_meta_[llevel]) ));
//	  return 0;
//  }

  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };
  int UpdateLazyLevels(int, VersionSet*, Compaction* const, std::vector<Output> &);

  inline void AdvanceActiveDeltaLevel(int llevel) {
	  delta_meta_[llevel].active++;
	  if (delta_meta_[llevel].active > hlsm::runtime::delta_level_num)
		  delta_meta_[llevel].active = 1;
  }

  inline void RollForwardDeltaLevels(int llevel) {
  	  delta_meta_[llevel].start = delta_meta_[llevel].clear;
  	  delta_meta_[llevel].clear = delta_meta_[llevel].active;
  	  delta_meta_[llevel].active++;
  	  if (delta_meta_[llevel].active > hlsm::runtime::delta_level_num)
  		  delta_meta_[llevel].active = 1;
  }

 private:
  friend class VersionSet;
  friend class LazyVersionSet;

  typedef std::set< std::pair<int, uint64_t> > DeletedFileSet;

  // lazy version does not actively perform compaction
  DeletedFileSet deleted_files_lazy_;
  std::vector< std::pair<int, FileMetaData> > new_files_lazy_;
  hlsm::delta_meta_t delta_meta_[hlsm::runtime::kLogicalLevels];
};

}  // namespace leveldb

#endif
