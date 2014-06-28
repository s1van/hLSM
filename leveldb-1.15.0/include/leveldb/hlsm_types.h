#ifndef HLSM_TYPES_H
#define HLSM_TYPES_H

#include <sys/queue.h>
#include <unistd.h>
#include <pthread.h>
#include <string>
#include <tr1/unordered_map>

#include "util/hash.h"
#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "leveldb/hlsm_debug.h"

namespace hlsm {

class FullMirror_PosixWritableFile : public leveldb::WritableFile {
private:
 std::string filename_;
 std::string sfilename_; // on secondary storage (HDD)

 FILE* file_;
 FILE* sfile_;
 int sfd_;

 leveldb::WritableFile *fp_;
 leveldb::WritableFile *sfp_;

public:
	FullMirror_PosixWritableFile(const std::string&, FILE*);
	~FullMirror_PosixWritableFile();
	virtual leveldb::Status Append(const leveldb::Slice&);
	virtual leveldb::Status Close();
	virtual leveldb::Status Flush();
	virtual leveldb::Status Sync();
};


}

/************************** Asynchronous Mirror I/O *****************************/

//1. Status Append(const Slice& data)
//2. Status Sync()
//3. Status Close()
typedef enum { MAppend = 1, MSync, MClose, MDelete, MHalt, MBufSync, MBufClose, MTruncate, MCopyFile, MCopyDeletedFile} mio_op_t;

typedef struct {
	mio_op_t type;
	void* ptr1;
	void* ptr2;
	int fd;
	size_t size;
	uint64_t offset;
} *mio_op, mio_op_s;

struct entry_ {
	mio_op op;
	TAILQ_ENTRY(entry_) entries_;
};

typedef struct entry_ entry_s;

typedef struct {
	pthread_mutex_t mutex;
	TAILQ_HEAD(tailhead, entry_) head;
	size_t length;

	pthread_cond_t noop; //no operation
	pthread_mutex_t cond_m;
} *opq, opq_s;

#define OPQ_MALLOC	(opq) malloc(sizeof(opq_s))

#define OPQ_NONEMPTY(q_)	(( (q_->head).tqh_first ))

#define OPQ_INIT(q_) 	do {		\
		pthread_mutex_init(&(q_->mutex), NULL);	\
		pthread_mutex_init(&(q_->cond_m), NULL);\
		pthread_cond_init(&(q_->noop), NULL);	\
		TAILQ_INIT(&(q_->head));	\
		q_->length = 0;	\
	} while(0)

#define OPQ_GET_LENGTH(q_)	((q_->length))

#define OPQ_WAIT(q_) do { \
		pthread_mutex_lock(&(q_->cond_m) );	\
		pthread_cond_wait(&(q_->noop), &(q_->cond_m) ); 	\
		pthread_mutex_unlock(&(q_->cond_m) );\
	} while(0)

#define OPQ_WAKEUP(q_) do { \
		pthread_mutex_lock(&(q_->cond_m) );	\
		pthread_cond_signal(&(q_->noop) ); \
		pthread_mutex_unlock(&(q_->cond_m) );\
	} while(0)

#define OPQ_ADD(q_, op_)	do {	\
		struct entry_ *e_;\
		e_ = (struct entry_ *) malloc(sizeof(struct entry_));	\
		e_->op = op_;	\
		pthread_mutex_lock(&(q_->mutex) );	\
		TAILQ_INSERT_TAIL(&(q_->head), e_, entries_);	\
		q_->length++;	\
		pthread_mutex_unlock(&(q_->mutex) );\
		OPQ_WAKEUP(q_);	\
	} while(0)

#define OPQ_ADD_TRUNCATE(q_, fd_, size_)	do{	\
		mio_op op_ = (mio_op)malloc(sizeof(mio_op_s));	\
		op_->type = MTruncate;\
		op_->fd = fd_;	\
		op_->size = size_;	\
		OPQ_ADD(q_, op_);	\
	} while(0)

#define OPQ_ADD_SYNC(q_, mfp_)	do{	\
		mio_op op_ = (mio_op)malloc(sizeof(mio_op_s));	\
		op_->type = MSync;\
		op_->ptr1 = mfp_;	\
		OPQ_ADD(q_, op_);	\
	} while(0)

#define OPQ_ADD_BUF_SYNC(q_, buf_, size_, fd_, off_)	do{	\
		mio_op op_ = (mio_op)malloc(sizeof(mio_op_s));	\
		op_->type = MBufSync;\
		op_->ptr1 = buf_;	\
		op_->size = size_;\
		op_->fd = fd_;		\
		op_->offset = off_;	\
		OPQ_ADD(q_, op_);	\
	} while(0)

#define OPQ_ADD_CLOSE(q_, mfp_)	do{	\
		mio_op op_ = (mio_op)malloc(sizeof(mio_op_s));	\
		op_->type = MClose;	\
		op_->ptr1 = mfp_;	\
		OPQ_ADD(q_, op_);	\
	} while(0)

#define OPQ_ADD_BUF_CLOSE(q_, fp_, fname_)	do{	\
		mio_op op_ = (mio_op)malloc(sizeof(mio_op_s));	\
		op_->type = MBufClose;\
		op_->ptr1 = fp_;	\
		op_->ptr2 = fname_; \
		OPQ_ADD(q_, op_);	\
	} while(0)

#define OPQ_ADD_DELETE(q_, fname_)	do{	\
		mio_op op_ = (mio_op)malloc(sizeof(mio_op_s));	\
		op_->type = MDelete;		\
		op_->ptr1 = (void*)fname_;	\
		OPQ_ADD(q_, op_);		\
	} while(0)

#define OPQ_ADD_HALT(q_)	do{	\
		mio_op op_ = (mio_op)malloc(sizeof(mio_op_s));	\
		op_->type = MHalt;		\
		OPQ_ADD(q_, op_);		\
	} while(0)

#define OPQ_ADD_APPEND(q_, mfp_, slice_)do{	\
		mio_op op_ = (mio_op)malloc(sizeof(mio_op_s));	\
		op_->type = MAppend;	\
		op_->ptr1 = mfp_;	\
		op_->ptr2 = (void *)slice_;	\
		OPQ_ADD(q_, op_);	\
	} while(0)

#define OPQ_ADD_COPYFILE(q_, fname_)	do{	\
		mio_op op_ = (mio_op)malloc(sizeof(mio_op_s));	\
		op_->type = MCopyFile;		\
		op_->ptr1 = (void*)fname_;	\
		OPQ_ADD(q_, op_);		\
	} while(0)

#define OPQ_ADD_COPY_DELETED_FILE(q_, fname_, fnum)	do{	\
		mio_op op_ = (mio_op)malloc(sizeof(mio_op_s));	\
		op_->type = MCopyDeletedFile;		\
		op_->ptr1 = (void*)fname_;	\
		op_->offset = fnum;		\
		OPQ_ADD(q_, op_);		\
	} while(0)

#define OPQ_POP(q_, op_) do{	\
		struct entry_ *e_;				\
		pthread_mutex_lock(&(q_->mutex) );	\
		e_ = (q_->head.tqh_first);\
		TAILQ_REMOVE(&(q_->head), (q_->head).tqh_first, entries_);\
		q_->length--;	\
		pthread_mutex_unlock(&(q_->mutex) );\
		op_ = e_->op;	\
		free(e_);		\
	} while(0)

#define INIT_HELPER_AND_QUEUE(helper_, queue_)	\
	do { \
		if (helper_ == NULL) {  \
			helper_ = (pthread_t *) malloc(sizeof(pthread_t));   \
			queue_ = OPQ_MALLOC;\
			OPQ_INIT(queue_);   \
			pthread_create(helper_, NULL,  &hlsm::opq_helper, queue_);	\
		}\
	} while (0)


/************************** Configuration Related *****************************/
namespace hlsm {
typedef enum { Default =1, FullMirror, PartialMirror, bLSM, PartialbLSM ,hLSM, Unknown} mode_t;
class DBMode {
public:
	DBMode(mode_t m) {
		mode = m;
	}

	DBMode(std::string m) {
		set(m);
	}

	DBMode(const char * m) {
		set(std::string(m));
	}

	inline bool isDefault() {
		return (mode == Default);
	}

	inline bool isFullMirror() {
		return (mode == FullMirror);
	}

	inline bool isPartialMirror() {
		return (mode == PartialMirror);
	}

	inline bool isbLSM() {
		return (mode == bLSM);
	}

	inline bool isPartialbLSM() {
		return (mode == PartialbLSM);
	}

	inline bool ishLSM() {
		return (mode == hLSM);
	}

	void set(std::string mstr) {
		if (mstr.find("Default") != std::string::npos)
			mode = Default;
		else if (mstr.find("FullMirror") != std::string::npos)
			mode = FullMirror;
		else if (mstr.find("PartialMirror") != std::string::npos)
			mode = PartialMirror;
		else if (mstr.find("PartialbLSM") != std::string::npos)
			mode = PartialbLSM;
		else if (mstr.find("bLSM") != std::string::npos)
			mode = bLSM;
		else if (mstr.find("hLSM") != std::string::npos)
			mode = hLSM;
		else
			mode = Unknown;
	}

	int get() {
		return (int) mode;
	}

private:
	mode_t mode;
};

/*
 * Map File Number to Level
 */
class TableLevel {
public:
	TableLevel() {};
	int add(uint64_t, int);
	int get(uint64_t);
	uint64_t getLatest();
	int remove(uint64_t);
	bool withinMirroredLevel(uint64_t);
	bool withinPureMirroredLevel(uint64_t);
private:
	std::tr1::unordered_map<uint64_t, int> mapping_;
	uint64_t latest;
};

namespace runtime {
/*
 * Check if a file is currently written to secondary storage
 */
class FileNameHash {
#define HSIZE 4096
private:
	static uint32_t hash[HSIZE];

public:
	static int add(const std::string filename) {
		DEBUG_INFO(3,"HashAdd %s\n", filename.c_str());
		uint32_t h = leveldb::Hash(filename.c_str(), filename.length(), 1);
		hash[h%HSIZE]++;
		return 0;
	}

	static int drop(const std::string filename) {
		DEBUG_INFO(3,"HashDrop %s\n", filename.c_str());
		uint32_t h = leveldb::Hash(filename.c_str(), filename.length(), 1);
		hash[h%HSIZE]--;

		return 0;
	}

	static int inuse(const std::string filename) {
		uint32_t h = leveldb::Hash(filename.c_str(), filename.length(), 1);
		return (hash[h%HSIZE]>0);
	}

#undef HSIZE
};

} // runtime

struct DeltaLevelMeta{
	uint32_t start; // start + 1 is the first delta level in use
	uint32_t clear; // clear until this level
	uint32_t active;

	DeltaLevelMeta(): start(0), clear(0), active(1) {}
};

typedef struct DeltaLevelMeta delta_meta_t;

} // hlsm

#endif  //HLSM_TYPES_H
