#include <assert.h>
#include <malloc.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>

#include "leveldb/hlsm_types.h"
#include "leveldb/hlsm_debug.h"
#include "leveldb/hlsm.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "leveldb/env.h"

#define USE_OPQ hlsm::config::use_opq_thread
#define SSPATH hlsm::config::secondary_storage_path
#define OPQ_HELPER hlsm::runtime::opq_helper
#define OPQ hlsm::runtime::op_queue

using namespace leveldb;
namespace hlsm{

// Roundup x to a multiple of y
static size_t Roundup(size_t x, size_t y) {
  return ((x + y - 1) / y) * y;
}

static Status IOError(const std::string& context, int err_number) {
  return Status::IOError(context, strerror(err_number));
}

static void *opq_helper(void * arg) {
	opq op_queue = (opq) arg;
	mio_op op;
	leveldb::WritableFile *sfp;
	int c = 0;

	DEBUG_INFO(1, "Start OPQ Helper\tQueue: %p\n", op_queue);
	while(1) {
		if (OPQ_NONEMPTY(op_queue)) {

			OPQ_POP(op_queue, op);			//operation
			DEBUG_INFO(3, "OPQ POP\ttype: %d\top: %p\n", op->type, op);

			if (op->type == MSync) {
				sfp = (WritableFile*) op->ptr1;	//file handler
				Status s = sfp->Sync();
				DEBUG_INFO(3, "MSync\tfp: %p\tstatus: %s\n", sfp, s.ToString().c_str());

			} else if (op->type == MBufSync) {
				char* buf = (char*) op->ptr1;	//buffer to sync
				size_t size = op->size;	//buffer size
				int fd = op->fd;	//file descriptor
				uint64_t offset = op->offset;	//corresponding offset
				ssize_t ret = pwrite(fd, buf, size, offset);
				free(buf);

			} else if (op->type == MBufClose) {
				FILE * fp = (FILE *) op->ptr1;
				assert(fclose(fp) == 0);
				DEBUG_INFO(3, "MBufClose\tfp: %p\n", fp);

			} else if (op->type == MTruncate) {
				size_t size = op->size;	//file size
				int fd = op->fd;	//file descriptor
				int ret = ftruncate(fd, size);

			} else if (op->type == MAppend) {
				sfp = (WritableFile*) op->ptr1;	//file handler
				Status s = sfp->Append(*((const Slice *) op->ptr2));
				free((void*) (((const Slice *) op->ptr2)->data() ));	//it is malloc-ed
				delete ((Slice *) op->ptr2);
				DEBUG_INFO(3, "MAppend\tsize: %ld\tstatus: %s\n", ((const Slice *) op->ptr2)->size(), s.ToString().c_str());

			} else if (op->type == MClose) {
				sfp = (WritableFile *) op->ptr1;	//file handler
				Status s = sfp->Close();
				DEBUG_INFO(3, "MClose\top: %p\tstatus: %s\n", op, s.ToString().c_str());

			} else if (op->type == MDelete) {
				std::string *fname = (std::string*) (op->ptr1);
				int ret = unlink(fname->c_str());
				DEBUG_INFO(3, "MDelete\tfname: %s\n", fname->c_str());
				delete fname;

			} else if (op->type == MHalt) {
				DEBUG_INFO(3, "MHalt");
				break;
			}

			free(op);
			continue;
		}

		OPQ_WAIT(op_queue);
		DEBUG_INFO(3, "Helper Count: %d\n", c++);
	}

	DEBUG_INFO(1, "Stop OPQ Helper\tQueue: %p\n", op_queue);
  return NULL;
}


class PosixWritableFile : public leveldb::WritableFile {
 private:
  std::string filename_;
  FILE* file_;

 public:
  PosixWritableFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }

  ~PosixWritableFile() {
    if (file_ != NULL) {
      // Ignoring any potential errors
      fclose(file_);
    }
  }

  virtual Status Append(const Slice& data) {
    size_t r = fwrite_unlocked(data.data(), 1, data.size(), file_);
    if (r != data.size()) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }

  virtual Status Close() {
    Status result;
    if (fclose(file_) != 0) {
      result = IOError(filename_, errno);
    }
    file_ = NULL;
    return result;
  }

  virtual Status Flush() {
    if (fflush_unlocked(file_) != 0) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }

  Status SyncDirIfManifest() {
    const char* f = filename_.c_str();
    const char* sep = strrchr(f, '/');
    Slice basename;
    std::string dir;
    if (sep == NULL) {
      dir = ".";
      basename = f;
    } else {
      dir = std::string(f, sep - f);
      basename = sep + 1;
    }
    Status s;
    if (basename.starts_with("MANIFEST")) {
      int fd = open(dir.c_str(), O_RDONLY);
      if (fd < 0) {
        s = IOError(dir, errno);
      } else {
        if (fsync(fd) < 0) {
          s = IOError(dir, errno);
        }
        close(fd);
      }
    }
    return s;
  }

  virtual Status Sync() {
    // Ensure new files referred to by the manifest are in the filesystem.
    Status s = SyncDirIfManifest();
    if (!s.ok()) {
      return s;
    }
    if (fflush_unlocked(file_) != 0 ||
        fdatasync(fileno(file_)) != 0) {
      s = Status::IOError(filename_, strerror(errno));
    }
    return s;
  }
};


class PosixBufferFile : public leveldb::WritableFile {
 private:
	std::string filename_;
	FILE* file_;
	int fd_;

	int buffer_size_;
	char* base_;            // The mapped region
	char* limit_;           // Limit of the mapped region
	char* dst_;             // Where to write next  (in range [base_,limit_])
	uint64_t file_offset_;  // Offset of base_ in file

 public:
  PosixBufferFile(const std::string& fname, FILE* f)
 	 : filename_(fname), file_(f),
       file_offset_(0){
		buffer_size_ = 4<<20;
		base_ = (char*) memalign(BLKSIZE,buffer_size_);
		dst_ = base_;
		limit_ = base_ + buffer_size_;
		fd_ = fileno(f);
		DEBUG_INFO(2, "%s\n", filename_.c_str());
		FileNameHash::add(filename_);
  }


  ~PosixBufferFile() {
    if (file_ != NULL) {
    	FileNameHash::drop(filename_);
    	PosixBufferFile::Close();
    }
  }

  virtual Status Append(const Slice& data) {
    const char* src = data.data();

    size_t left = data.size();
    while (left > 0) {
      assert(base_ <= dst_);
      assert(dst_ <= limit_);
      size_t avail = limit_ - dst_;
      if (avail == 0) {
    	  OPQ_ADD_BUF_SYNC(OPQ, base_, dst_-base_, fd_, file_offset_);
    	  file_offset_ += limit_ - base_;
    	  base_ = (char*) memalign(BLKSIZE,buffer_size_);
    	  dst_ = base_;
    	  limit_ = base_ + buffer_size_;
      }

      size_t n = (left <= avail) ? left : avail;
      memcpy(dst_, src, n);
      dst_ += n;
      src += n;
      left -= n;
    }

    return Status::OK();
  }

  virtual Status Close() {
    Status s;
    OPQ_ADD_BUF_SYNC(OPQ, base_, Roundup(dst_-base_, BLKSIZE), fd_, file_offset_);
    OPQ_ADD_TRUNCATE(OPQ, fd_, file_offset_ + dst_-base_);
    OPQ_ADD_BUF_CLOSE(OPQ, file_); // pass file_ to make a clean closure

    file_=NULL;
    base_ = NULL;
    limit_ = NULL;
    return s;
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {

    return Status::OK();
  }
};


FullMirror_PosixWritableFile::FullMirror_PosixWritableFile(const std::string& fname, FILE* f)
 	 : filename_(fname), file_(f) {
	sfilename_ = std::string(SSPATH) + fname.substr(fname.find_last_of("/"));
	if (hlsm::config::direct_write_on_secondary) {
		sfd_ = open(sfilename_.c_str(), O_CREAT | O_RDWR | O_TRUNC| O_DIRECT, 0777);
		sfile_ = fdopen(sfd_, "w" );
	} else {
		sfd_ = open(sfilename_.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0777);
		sfile_ = fdopen(sfd_, "w" );
	}
	DEBUG_INFO(2,"Primary: %s\t%p\tSecondary: %s\t%p\t%d\n",filename_.c_str(), file_, sfilename_.c_str(), sfile_, sfd_);

	if (USE_OPQ) {
		INIT_HELPER_AND_QUEUE(OPQ_HELPER, OPQ);
	}

	if (hlsm::config::secondary_use_buffer_file) {
		sfp_ = new PosixBufferFile(sfilename_, sfile_);

	} else {
		sfp_ = new PosixWritableFile(sfilename_, sfile_);
	}

    fp_ = new PosixWritableFile(filename_, file_);
    DEBUG_INFO(2,"%s\n",filename_.c_str());
  }


FullMirror_PosixWritableFile::~FullMirror_PosixWritableFile() {
    if (file_ != NULL) {
      Close();
    }
  }

  Status FullMirror_PosixWritableFile::Append(const Slice& data) {
    if (USE_OPQ) {
    	Slice *sdata = data.clone();
    	OPQ_ADD_APPEND(OPQ, sfp_, sdata);
	} else {
		Status ss = sfp_->Append(data);
		if (!ss.ok())
			return ss;
	}

    Status s = fp_->Append(data);
    DEBUG_INFO(3, "%ld\n", data.size());
    return s;
  }

  Status FullMirror_PosixWritableFile::Close() {
	if (USE_OPQ) {
		OPQ_ADD_CLOSE(OPQ, sfp_);
	} else {
		Status ss = sfp_->Close();
		if (!ss.ok())
			return ss;
	}

	Status s = fp_->Close();
	DEBUG_INFO(2, "%s\n", s.ToString().c_str());
	file_ = NULL;
	return s;
  }

  Status FullMirror_PosixWritableFile::Flush() {
    return Status::OK();
  }

  Status FullMirror_PosixWritableFile::Sync() {
    DEBUG_INFO(3, "BGN\t%s\t%s\n", filename_.c_str(), sfilename_.c_str());
    if (USE_OPQ && !hlsm::config::lazy_sync_on_secondary) {
    	OPQ_ADD_SYNC(OPQ, sfp_);
    }
    Status s = fp_->Sync();

    DEBUG_INFO(3, "END\t%s\t%s\n", filename_.c_str(), sfilename_.c_str());
    return s;
  }
} // hlsm


