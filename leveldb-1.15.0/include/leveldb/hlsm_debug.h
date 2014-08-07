#ifndef HLSM_DEBUG_H
#define HLSM_DEBUG_H

#include <iostream>
#include <stdio.h>
#include <sys/time.h>
#include "port/port_posix.h"

/**************** private ****************/
namespace hlsm {
namespace config {
extern int debug_level;
} // config

namespace runtime {
extern FILE *debug_fd;
extern leveldb::port::Mutex debug_mutex_;
} // runtime
}

#define _DEBUG_FD hlsm::runtime::debug_fd
#define _FLUSH do {fflush(_DEBUG_FD);} while(0)

#define _PRINT_CURRENT_TIME	do {\
		struct timeval now;     \
		gettimeofday(&now, NULL);\
		now.tv_sec = (now.tv_sec << 36) >> 36;		\
		fprintf(_DEBUG_FD, "%ld", now.tv_sec * 1000000 + now.tv_usec);\
		_FLUSH; \
	} while(0)
#define _PRINT_LOC_INFO	do{	\
		fprintf(_DEBUG_FD, "[%s,\t%s: %d]", __FUNCTION__, __FILE__, __LINE__);	\
		_FLUSH; \
	} while(0)


#define _DEBUG_MEASURE(_func, _tag) do{\
		struct timeval before;  \
		struct timeval after;   \
		gettimeofday(&before, NULL);\
		before.tv_sec = (before.tv_sec << 36) >> 36;\
		_func;			\
		gettimeofday(&after, NULL);	\
		after.tv_sec = (after.tv_sec << 36) >> 36;\
		hlsm::runtime::debug_mutex_.Lock();  \
		_PRINT_CURRENT_TIME;        \
		fprintf(_DEBUG_FD, "\t");   \
		_PRINT_LOC_INFO;            \
		fprintf(_DEBUG_FD, "\t%s\t%ld\n", _tag, (after.tv_sec - before.tv_sec) * 1000000 + after.tv_usec - before.tv_usec);\
		_FLUSH; \
		hlsm::runtime::debug_mutex_.Unlock();\
	} while(0)

#define _DEBUG_MEASURE_RECORD(_func, _tag) do{\
		struct timeval before;  \
		struct timeval after;   \
		gettimeofday(&before, NULL);\
		before.tv_sec = (before.tv_sec << 36) >> 36;\
		_func;			\
		gettimeofday(&after, NULL);	\
		after.tv_sec = (after.tv_sec << 36) >> 36;\
		hlsm::runtime::counters.add(_tag, (after.tv_sec - before.tv_sec) * 1000000 + after.tv_usec - before.tv_usec );	\
	} while(0)

#define _DEBUG_PRINT(_format, ...) do{\
		fprintf(_DEBUG_FD, _format, ## __VA_ARGS__);\
		_FLUSH; \
	} while(0)

#define _DEBUG_INFO(_format, ...) do{\
		_PRINT_CURRENT_TIME;         \
		fprintf(_DEBUG_FD, "\t");    \
		_PRINT_LOC_INFO;             \
		fprintf(_DEBUG_FD, _format, ## __VA_ARGS__);\
		_FLUSH; \
	} while(0)


#define _DEBUG_META_ITER(_tag, _vec) do{	\
		_PRINT_CURRENT_TIME;		\
		fprintf(_DEBUG_FD, "\t");   \
		_PRINT_LOC_INFO;			\
		fprintf(_DEBUG_FD, "\t%s", _tag);   \
		for(int _i = 0;_i<_vec.size(); _i++){   \
			fprintf(_DEBUG_FD, "\t%lu", _vec[_i]->number);   \
		}       \
		fprintf(_DEBUG_FD, "\n");   \
		_FLUSH; \
	} while(0)

#define _DEBUG_LEVEL_CHECK(_level, _do) do {        \
		hlsm::runtime::debug_mutex_.Lock();  \
		if (_level <= hlsm::config::debug_level) {  \
			_do;\
		}       \
		hlsm::runtime::debug_mutex_.Unlock();\
	} while(0)

#define _DEBUG_LEVEL_CHECK_NOLOCK(_level, _do) do {        \
		if (_level <= hlsm::config::debug_level) {  \
			_do;\
		}       \
	} while(0)


#define _DEBUG_LEVEL_CHECK_NOLOCK_NOSKIP(_level, _do_if_true, _do_if_false) do {        \
		if (_level <= hlsm::config::debug_level) {  \
			_do_if_true;	\
		} else {      	\
			_do_if_false;	\
		}	\
	} while(0)


/*
 * Public Functions
 */


#ifndef NDEBUG

// with lock
#define DEBUG_MEASURE(_level, _do, ...) _DEBUG_LEVEL_CHECK_NOLOCK_NOSKIP(_level, _DEBUG_MEASURE(_do, __VA_ARGS__), _do) // locked within _DEBUG_MEASURE
#define DEBUG_MEASURE_RECORD(_level, _do, ...) _DEBUG_LEVEL_CHECK_NOLOCK_NOSKIP(_level, _DEBUG_MEASURE_RECORD(_do, __VA_ARGS__), _do)
#define DEBUG_PRINT(_level, ...) _DEBUG_LEVEL_CHECK(_level, _DEBUG_PRINT(__VA_ARGS__))
#define DEBUG_INFO(_level, ...) _DEBUG_LEVEL_CHECK(_level, _DEBUG_INFO(__VA_ARGS__))
#define DEBUG_META_ITER(_level, ...) _DEBUG_LEVEL_CHECK(_level, _DEBUG_META_ITER(__VA_ARGS__))
#define DEBUG_LEVEL_CHECK(_level, _do) _DEBUG_LEVEL_CHECK(_level, _do)

// no lock
#define DEBUG_PRINT_NOLOCK(_level, ...) _DEBUG_LEVEL_CHECK_NOLOCK(_level, _DEBUG_PRINT(__VA_ARGS__))
#define DEBUG_INFO_NOLOCK(_level, ...) _DEBUG_LEVEL_CHECK_NOLOCK(_level, _DEBUG_INFO(__VA_ARGS__))
#define DEBUG_META_ITER_NOLOCK(_level, ...) _DEBUG_LEVEL_CHECK_NOLOCK(_level, _DEBUG_META_ITER(__VA_ARGS__))
#define DEBUG_LEVEL_CHECK_NOLOCK(_level, _do) _DEBUG_LEVEL_CHECK_NOLOCK(_level, _do)

#define DEBUG_BULK_START	hlsm::runtime::debug_mutex_.Lock()
#define DEBUG_BULK_END	hlsm::runtime::debug_mutex_.Unlock()

#else // ifdef NDEBUG

#define _DO_NOTHING	do{} while(0)

// with lock
#define DEBUG_MEASURE(_level, _func, ...) do{_func;} while(0)
#define DEBUG_MEASURE_RECORD(_level, _func, ...) do{_func;} while(0)
#define DEBUG_PRINT(...) 	_DO_NOTHING
#define DEBUG_INFO(...) 	_DO_NOTHING
#define DEBUG_META_ITER(...) 	_DO_NOTHING
#define DEBUG_LEVEL_CHECK(...) _DO_NOTHING

// no lock
#define DEBUG_PRINT_NOLOCK(...) _DO_NOTHING
#define DEBUG_INFO_NOLOCK(...)	_DO_NOTHING
#define DEBUG_META_ITER_NOLOCK(...) 	_DO_NOTHING
#define DEBUG_LEVEL_CHECK_NOLOCK(...)	_DO_NOTHING

#define DEBUG_BULK_START	_DO_NOTHING
#define DEBUG_BULK_END		_DO_NOTHING

#endif

#endif  //HLSM_DEBUG_H
