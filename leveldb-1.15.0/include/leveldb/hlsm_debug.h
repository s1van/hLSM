#ifndef HLSM_DEBUG_H
#define HLSM_DEBUG_H

#include <iostream>
#include <stdio.h>
#include <sys/time.h>

/**************** private ****************/
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
		_PRINT_CURRENT_TIME;        \
		fprintf(_DEBUG_FD, "\t");   \
		_PRINT_LOC_INFO;            \
		struct timeval before;  \
		struct timeval after;   \
		gettimeofday(&before, NULL);\
		before.tv_sec = (before.tv_sec << 36) >> 36;\
		_func;			\
		gettimeofday(&after, NULL);	\
		after.tv_sec = (after.tv_sec << 36) >> 36;\
		fprintf(_DEBUG_FD, "\t%s\t%ld\n", _tag, (after.tv_sec - before.tv_sec) * 1000000 + after.tv_usec - before.tv_usec);\
		_FLUSH; \
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
		if (_level <= hlsm::config::debug_level) {  \
			_do;\
		}       \
	} while(0)

/**************** public ****************/
#define DEBUG_MEASURE(_level, ...) _DEBUG_LEVEL_CHECK(_level, _DEBUG_MEASURE(__VA_ARGS__))
#define DEBUG_PRINT(_level, ...) _DEBUG_LEVEL_CHECK(_level, _DEBUG_PRINT(__VA_ARGS__))
#define DEBUG_INFO(_level, ...) _DEBUG_LEVEL_CHECK(_level, _DEBUG_INFO(__VA_ARGS__))
#define DEBUG_META_ITER(_level, ...) _DEBUG_LEVEL_CHECK(_level, _DEBUG_META_ITER(__VA_ARGS__))

#endif  //HLSM_DEBUG_H
