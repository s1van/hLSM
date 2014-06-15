#ifndef HLSM_LAZY_VERSION_EDIT_H_
#define HLSM_LAZY_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>
#include "db/dbformat.h"
#include "db/version_edit.h"
#include "leveldb/hlsm.h"

namespace leveldb {

class VersionSet;

class LazyVersionEdit: leveldb::VersionEdit {
 public:
	LazyVersionEdit();
    void Clear();

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level, uint64_t file,
               uint64_t file_size,
               const InternalKey& smallest,
               const InternalKey& largest, bool for_lazy_version = false) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    if (for_lazy_version)
    	new_files_lazy_.push_back(std::make_pair(level, f));
    else
    	new_files_.push_back(std::make_pair(level, f));
  }

  // Delete the specified "file" from the specified "level".
  void DeleteFile(int level, uint64_t file, bool for_lazy_version = false) {
	if (for_lazy_version)
		deleted_files_lazy_.insert(std::make_pair(level, file));
	else
		deleted_files_.insert(std::make_pair(level, file));
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;
  friend class LazyVersionSet;

  typedef std::set< std::pair<int, uint64_t> > DeletedFileSet;

  // lazy version does not actively perform compaction
  DeletedFileSet deleted_files_lazy_;
  std::vector< std::pair<int, FileMetaData> > new_files_lazy_;
};

}  // namespace leveldb

#endif
