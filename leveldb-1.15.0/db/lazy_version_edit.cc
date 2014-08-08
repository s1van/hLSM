// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"
#include "db/lazy_version_edit.h"
#include "db/version_set.h"
#include "db/lazy_version_set.h"
#include "db/hlsm_impl.h"
#include "util/coding.h"
#include "leveldb/hlsm_func.h"

namespace leveldb {

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed.
enum Tag {
  kComparator           = 1,
  kLogNumber            = 2,
  kNextFileNumber       = 3,
  kLastSequence         = 4,
  kCompactPointer       = 5,
  kDeletedFile          = 6,
  kNewFile              = 7,
  // 8 was used for large value refs
  kPrevLogNumber        = 9,
  kDeletedLazyFile		= 10,
  kNewLazyFile          = 11,
  kDeltaLevelOffset		= 12
};

LazyVersionEdit::LazyVersionEdit() {Clear();}
LazyVersionEdit::LazyVersionEdit(VersionSet* v) {
	Clear();
	SetDeltaLevels(v);
}

void LazyVersionEdit::Clear() {
  comparator_.clear();
  log_number_ = 0;
  prev_log_number_ = 0;
  last_sequence_ = 0;
  next_file_number_ = 0;
  has_comparator_ = false;
  has_log_number_ = false;
  has_prev_log_number_ = false;
  has_next_file_number_ = false;
  has_last_sequence_ = false;
  deleted_files_.clear();
  new_files_.clear();
  deleted_files_lazy_.clear();
  new_files_lazy_.clear();
}

void LazyVersionEdit::EncodeTo(std::string* dst) const {
  if (has_comparator_) {
    PutVarint32(dst, kComparator);
    PutLengthPrefixedSlice(dst, comparator_);
  }
  if (has_log_number_) {
    PutVarint32(dst, kLogNumber);
    PutVarint64(dst, log_number_);
  }
  if (has_prev_log_number_) {
    PutVarint32(dst, kPrevLogNumber);
    PutVarint64(dst, prev_log_number_);
  }
  if (has_next_file_number_) {
    PutVarint32(dst, kNextFileNumber);
    PutVarint64(dst, next_file_number_);
  }
  if (has_last_sequence_) {
    PutVarint32(dst, kLastSequence);
    PutVarint64(dst, last_sequence_);
  }

  for (size_t i = 0; i < compact_pointers_.size(); i++) {
    PutVarint32(dst, kCompactPointer);
    PutVarint32(dst, compact_pointers_[i].first);  // level
    PutLengthPrefixedSlice(dst, compact_pointers_[i].second.Encode());
  }

  for (size_t i = 0; i < hlsm::runtime::kLogicalLevels; i++) {
    PutVarint32(dst, kDeltaLevelOffset);
    PutVarint32(dst, i);  // level
    PutVarint32(dst, delta_meta_[i].start);
    PutVarint32(dst, delta_meta_[i].clear);
    PutVarint32(dst, delta_meta_[i].active);
  }

  for (DeletedFileSet::const_iterator iter = deleted_files_.begin();
       iter != deleted_files_.end();
       ++iter) {
    PutVarint32(dst, kDeletedFile);
    PutVarint32(dst, iter->first);   // level
    PutVarint64(dst, iter->second);  // file number
  }

  for (DeletedFileSet::const_iterator iter = deleted_files_lazy_.begin();
       iter != deleted_files_lazy_.end();
       ++iter) {
    PutVarint32(dst, kDeletedLazyFile);
    PutVarint32(dst, iter->first);   // level
    PutVarint64(dst, iter->second);  // file number
  }

  for (size_t i = 0; i < new_files_.size(); i++) {
    const FileMetaData& f = new_files_[i].second;
    PutVarint32(dst, kNewFile);
    PutVarint32(dst, new_files_[i].first);  // level
    PutVarint64(dst, f.number);
    PutVarint64(dst, f.file_size);
    PutLengthPrefixedSlice(dst, f.smallest.Encode());
    PutLengthPrefixedSlice(dst, f.largest.Encode());
  }

  for (size_t i = 0; i < new_files_lazy_.size(); i++) {
    const FileMetaData& f = new_files_lazy_[i].second;
    PutVarint32(dst, kNewLazyFile);
    PutVarint32(dst, new_files_lazy_[i].first);  // level
    PutVarint64(dst, f.number);
    PutVarint64(dst, f.file_size);
    PutLengthPrefixedSlice(dst, f.smallest.Encode());
    PutLengthPrefixedSlice(dst, f.largest.Encode());
  }
}

static bool GetInternalKey(Slice* input, InternalKey* dst) {
  Slice str;
  if (GetLengthPrefixedSlice(input, &str)) {
    dst->DecodeFrom(str);
    return true;
  } else {
    return false;
  }
}

static bool GetLevel(Slice* input, int* level) {
  uint32_t v;
  if (GetVarint32(input, &v) &&
      v < config::kNumLevels) {
    *level = v;
    return true;
  } else {
    return false;
  }
}

static bool GetLazyLevel(Slice* input, int* level) {
  uint32_t v;
  if (GetVarint32(input, &v) &&
      v < hlsm::runtime::kNumLazyLevels) {
    *level = v;
    return true;
  } else {
    return false;
  }
}

Status LazyVersionEdit::DecodeFrom(const Slice& src) {
  Clear();
  Slice input = src;
  const char* msg = NULL;
  uint32_t tag;
  uint32_t start, clear, active;

  // Temporary storage for parsing
  int level;
  uint64_t number;
  FileMetaData f;
  Slice str;
  InternalKey key;

  while (msg == NULL && GetVarint32(&input, &tag)) {
    switch (tag) {
      case kComparator:
        if (GetLengthPrefixedSlice(&input, &str)) {
          comparator_ = str.ToString();
          has_comparator_ = true;
        } else {
          msg = "comparator name";
        }
        break;

      case kDeltaLevelOffset:
        if (GetLazyLevel(&input, &level) &&
        	GetVarint32(&input, &start)  &&
        	GetVarint32(&input, &clear)  &&
        	GetVarint32(&input, &active)) {
        	DEBUG_INFO(3, "level: %d, start: %u, clear: %u, active: %u\n"
        			, level, start, clear, active);
        	delta_meta_[level].set_delta_meta(start, clear, active);
        	assert(start <= hlsm::runtime::delta_level_num &&
        		   clear <= hlsm::runtime::delta_level_num &&
        		   active <= hlsm::runtime::delta_level_num);
        } else {
          msg = "delta level offset";
        }
        break;

      case kLogNumber:
        if (GetVarint64(&input, &log_number_)) {
          has_log_number_ = true;
        } else {
          msg = "log number";
        }
        break;

      case kPrevLogNumber:
        if (GetVarint64(&input, &prev_log_number_)) {
          has_prev_log_number_ = true;
        } else {
          msg = "previous log number";
        }
        break;

      case kNextFileNumber:
        if (GetVarint64(&input, &next_file_number_)) {
          has_next_file_number_ = true;
        } else {
          msg = "next file number";
        }
        break;

      case kLastSequence:
        if (GetVarint64(&input, &last_sequence_)) {
          has_last_sequence_ = true;
        } else {
          msg = "last sequence number";
        }
        break;

      case kCompactPointer:
        if (GetLevel(&input, &level) &&
            GetInternalKey(&input, &key)) {
          compact_pointers_.push_back(std::make_pair(level, key));
        } else {
          msg = "compaction pointer";
        }
        break;

      case kDeletedFile:
        if (GetLevel(&input, &level) &&
            GetVarint64(&input, &number)) {
          deleted_files_.insert(std::make_pair(level, number));
        } else {
          msg = "deleted file";
        }
        break;

      case kDeletedLazyFile:
        if (GetLazyLevel(&input, &level) &&
            GetVarint64(&input, &number)) {
          deleted_files_lazy_.insert(std::make_pair(level, number));
        } else {
          msg = "deleted lazy file";
        }
        break;

      case kNewFile:
        if (GetLevel(&input, &level) &&
            GetVarint64(&input, &f.number) &&
            GetVarint64(&input, &f.file_size) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest)) {
          new_files_.push_back(std::make_pair(level, f));
        } else {
          msg = "new-file entry";
        }
        break;

      case kNewLazyFile:
        if (GetLazyLevel(&input, &level) &&
            GetVarint64(&input, &f.number) &&
            GetVarint64(&input, &f.file_size) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest)) {
          new_files_lazy_.push_back(std::make_pair(level, f));
        } else {
          msg = "new-lazy-file entry";
        }
        break;

      default:
        msg = "unknown tag";
        break;
    }
  }

  if (msg == NULL && !input.empty()) {
    msg = "invalid tag";
  }

  Status result;
  if (msg != NULL) {
    result = Status::Corruption("VersionEdit", msg);
  }
  return result;
}

std::string LazyVersionEdit::DebugString() const {
  std::string r;
  r.append("LazyVersionEdit {");
  if (has_comparator_) {
    r.append("\n  Comparator: ");
    r.append(comparator_);
  }
  if (has_log_number_) {
    r.append("\n  LogNumber: ");
    AppendNumberTo(&r, log_number_);
  }
  if (has_prev_log_number_) {
    r.append("\n  PrevLogNumber: ");
    AppendNumberTo(&r, prev_log_number_);
  }
  if (has_next_file_number_) {
    r.append("\n  NextFile: ");
    AppendNumberTo(&r, next_file_number_);
  }
  if (has_last_sequence_) {
    r.append("\n  LastSeq: ");
    AppendNumberTo(&r, last_sequence_);
  }
  for (size_t i = 0; i < compact_pointers_.size(); i++) {
    r.append("\n  CompactPointer: ");
    AppendNumberTo(&r, compact_pointers_[i].first);
    r.append(" ");
    r.append(compact_pointers_[i].second.DebugString());
  }
  for (DeletedFileSet::const_iterator iter = deleted_files_.begin();
       iter != deleted_files_.end();
       ++iter) {
    r.append("\n  DeleteFile: ");
    AppendNumberTo(&r, iter->first);
    r.append(" ");
    AppendNumberTo(&r, iter->second);
  }

  for (DeletedFileSet::const_iterator iter = deleted_files_lazy_.begin();
       iter != deleted_files_lazy_.end();
       ++iter) {
    r.append("\n  DeleteLazyFile: ");
    AppendNumberTo(&r, iter->first);
    r.append(" ");
    AppendNumberTo(&r, iter->second);
  }

  for (size_t i = 0; i < new_files_lazy_.size(); i++) {
    const FileMetaData& f = new_files_lazy_[i].second;
    r.append("\n  AddLazyFile: ");
    AppendNumberTo(&r, new_files_lazy_[i].first);
    r.append(" ");
    AppendNumberTo(&r, f.number);
    r.append(" ");
    AppendNumberTo(&r, f.file_size);
    r.append(" ");
    r.append(f.smallest.DebugString());
    r.append(" .. ");
    r.append(f.largest.DebugString());
  }
  r.append("\n}\n");
  return r;
}

void LazyVersionEdit::SetDeltaLevels(VersionSet* v) {
	  SetDeltaLevels(reinterpret_cast<LazyVersionSet*>(v)
			  ->GetDeltaLevelOffsets() );
 }

int LazyVersionEdit::UpdateLazyLevels(int level, VersionSet* v, Compaction* const c, std::vector<Output> &outputs) {
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
		  LazyVersionSet* lazyV = reinterpret_cast<LazyVersionSet*>(v);
		  if (lazyV->isLazyLevelNonEmpty(hlsm::get_active_delta_level(delta_meta_, 1)))
			  AdvanceActiveDeltaLevel(1);

	  // X.L (f) -> (X+1).R
	  } else if (llevel == 0) {
		  assert(level == 1);
		for (int i = 0; i < c->num_input_files(0); i++) {
		  FileMetaData *f = c->input(0,i);
		  DeleteLazyFile(level, f->number);
		  AddLazyFile(hlsm::get_active_delta_level(delta_meta_, 1),
		  				f->number, f->file_size, f->smallest, f->largest);
		}

	  } else if (llevel > 0 && llevel < hlsm::runtime::two_phase_end_level) {
		for (int i = 0; i < c->num_input_files(0); i++) {
		  FileMetaData *f = c->input(0,i);
		  std::string *copy_from = new std::string(TableFileName(hlsm::config::primary_storage_path, f->number));
		  OPQ_ADD_COPYFILE(hlsm::runtime::op_queue, copy_from, f->number);
		  // add f to X.NEW
		  AddLazyFile(hlsm::get_hlsm_new_level(level),
				  f->number, f->file_size, f->smallest, f->largest);
		}

	  } else if (llevel == hlsm::runtime::two_phase_end_level) {
		  // delete input files from (X+1).R
		  for (int i = 0; i < c->num_input_files(1); i++) {
			  deleted_files_lazy_.insert(std::make_pair(
					  hlsm::get_pure_mirror_level(level+1), c->input(1, i)->number));
		  }
		  // add output files to (X+1).R
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

void LazyVersionEdit::AddLazyFileByRawLevel(int raw_level, uint64_t file,
		uint64_t file_size,
		const InternalKey& smallest,
		const InternalKey& largest,
		Version *lv) {

	int llevel = hlsm::get_logical_level(raw_level);

	// must be L0.L, purely mirrored
	if (llevel == 0) {
		AddLazyFile(raw_level, file, file_size, smallest, largest);

	// within the range of two-phase compaction, copy files in X.R to X.delta?, X.L to X.NEW
	} else if (llevel > 0 && llevel <= hlsm::runtime::two_phase_end_level){
		// X.L
		if (raw_level %2 == 1) {
			AddLazyFile(hlsm::get_hlsm_new_level(raw_level),
					file, file_size, smallest, largest);

		// X.R
		} else {
			int dlevel = hlsm::get_active_delta_level(delta_meta_, llevel);
			AddLazyFile(dlevel, file, file_size, smallest, largest);

			// check if current delta level is full (its size equals the size of the new level above)
			//	raw_level >= 2 due to llevel > 0
			if (hlsm::max_fnum_in_level(raw_level - 2) - 1 <= lv->NumFiles(dlevel) ) {
				AdvanceActiveDeltaLevel(llevel);
			}
		}

  // levels that are out of the control of the two-phase compaction
	} else if (llevel > hlsm::runtime::two_phase_end_level ) {
		AddLazyFile(hlsm::get_pure_mirror_level(raw_level),
				file, file_size, smallest, largest);
	}
}

}  // namespace leveldb
