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

class LazyVersionEdit: public leveldb::VersionEdit {
 public:
	LazyVersionEdit();
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
