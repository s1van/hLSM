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
		  hlsm::set_delta_meta(&delta_meta_[level], &meta[level]);
		  assert(hlsm::is_valid_detla_meta( &(delta_meta_[level]) ));
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
  int UpdateLazyLevels(int level, VersionSet* v, Compaction* const c, std::vector<Output> &outputs) {
	  int llevel = hlsm::get_logical_level(level);
	  DEBUG_INFO(2, "level: %d, llevel: %d\n", level, llevel);
	  SetDeltaLevels(v);

	  // L0.R -> L0.L
	  if (level == 0) {
		  // delete all files in (physical) level 0
		  for (int i = 0; i < c->num_input_files(0); i++) {
			  deleted_files_lazy_.insert(std::make_pair(level, c->input(0, i)->number));
		  }
		  // add all outputs to a delta sub-level in (logical) level 1
		  for (size_t i = 0; i < outputs.size(); i++) {
			  const Output& out = outputs[i];
			  AddLazyFile(1, out.number,
					  out.file_size, out.smallest, out.largest);
		  }

		  // update delta level meta
	      AdvanceActiveDeltaLevel(llevel + 1);

	  // X.L (f) -> (X+1).R
	  } else if (llevel == 0) {
		  assert(level == 1);
		  FileMetaData *f = c->input(0,0);
		  DeleteLazyFile(level, f->number);
		  AddLazyFile(hlsm::get_active_delta_level(delta_meta_, 1),
		  				f->number, f->file_size, f->smallest, f->largest);

	  } else if (llevel > 0 && llevel < hlsm::runtime::two_phase_end_level) {
		  FileMetaData *f = c->input(0,0);
		  std::string *copy_from = new std::string(TableFileName(hlsm::config::primary_storage_path, f->number));
		  hlsm::runtime::moving_tables_.insert(f->number);
		  OPQ_ADD_COPY_DELETED_FILE(hlsm::runtime::op_queue, copy_from, f->number);
		  // add f to X.NEW
		  AddLazyFile(hlsm::get_hlsm_new_level(level),
				  f->number, f->file_size, f->smallest, f->largest);

	  } else if (llevel == hlsm::runtime::two_phase_end_level) {
		  // delete input files from (X+1).R
		  for (int i = 0; i < c->num_input_files(1); i++) {
			  deleted_files_lazy_.insert(std::make_pair(
					  hlsm::get_pure_mirror_level(level+1), c->input(1, i)->number));
		  }
		  // add outputs to next level
		  for (size_t i = 0; i < outputs.size(); i++) {
			  const Output& out = outputs[i];
			  AddLazyFile(hlsm::get_pure_mirror_level(level+1), out.number,
					  out.file_size, out.smallest, out.largest);
		  }

	  } else if (llevel > hlsm::runtime::two_phase_end_level) {
		  // delete input files from current mirrored level
		  for (int i = 0; i < c->num_input_files(0); i++) {
			  deleted_files_lazy_.insert(std::make_pair(
					  hlsm::get_pure_mirror_level(level), c->input(0, i)->number));
		  }

		  // delete input files from next mirrored level
		  for (int i = 0; i < c->num_input_files(1); i++) {
			  deleted_files_lazy_.insert(std::make_pair(
					  hlsm::get_pure_mirror_level(level+1), c->input(1, i)->number));
		  }

		  // add outputs to next level
		  for (size_t i = 0; i < outputs.size(); i++) {
			  const Output& out = outputs[i];
			  AddLazyFile(hlsm::get_pure_mirror_level(level+1), out.number,
					  out.file_size, out.smallest, out.largest);
		  }
	  }

	  return 0;
  }

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
