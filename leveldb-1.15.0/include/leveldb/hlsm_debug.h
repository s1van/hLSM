#ifndef HLSM_DEBUG_H
#define HLSM_DEBUG_H

#include <iostream>
#include <stdio.h>
#include <sys/time.h>

#ifdef HLSM_DEBUG_DUMP

#define PRINT_FINISH do {std::cout.flush();} while(0)

#define PRINT_CURRENT_TIME	do {		\
		struct timeval now;		\
		gettimeofday(&now, NULL);	\
		now.tv_sec = (now.tv_sec << 36) >> 36;		\
		std::cout << now.tv_sec * 1000000 + now.tv_usec;\
		PRINT_FINISH; \
	} while(0)

#define PRINT_LOC_INFO	do{	\
		std::cout << "[" << __FUNCTION__ << ",\t"	\
		<< __FILE__ << ": " << __LINE__ << "]";		\
		PRINT_FINISH; \
	} while(0)

#define DEBUG_MEASURE(func, tag) do{		\
		PRINT_CURRENT_TIME; 	\
		std::cout << "\t";	\
		PRINT_LOC_INFO;		\
		struct timeval before;	\
		struct timeval after;	\
		gettimeofday(&before, NULL);	\
		before.tv_sec = (before.tv_sec << 36) >> 36;\
		func;			\
		gettimeofday(&after, NULL);	\
		after.tv_sec = (after.tv_sec << 36) >> 36;\
		std::cout << "\t" << tag << "\t" << (after.tv_sec - before.tv_sec) * 1000000 + after.tv_usec - before.tv_usec << std::endl;\
		PRINT_FINISH; \
	} while(0)

#define DEBUG_INFO(_str)  do{		\
		PRINT_CURRENT_TIME; 	\
		std::cout << "\t";	\
		PRINT_LOC_INFO;		\
		std::cout << "\t" << _str << std::endl;	\
		PRINT_FINISH; \
	} while(0)

#define DEBUG_INFO2(_arg1, _arg2)  do{	\
		PRINT_CURRENT_TIME;	\
		std::cout << "\t";	\
		PRINT_LOC_INFO;		\
		std::cout << _arg1 << "\t" << _arg2 << std::endl;\
		PRINT_FINISH; \
	} while(0)

#define DEBUG_INFO3(_arg1, _arg2, _arg3)  do{	\
		PRINT_CURRENT_TIME;		\
		std::cout << "\t";		\
		PRINT_LOC_INFO;			\
		std::cout << _arg1 << "\t" 	\
			<< _arg2 << "\t"	\
			<< _arg3 << std::endl;	\
		PRINT_FINISH; \
	} while(0)

#define DEBUG_META_VEC(_tag, _vec) do{		\
		PRINT_CURRENT_TIME;		\
		std::cout << "\t";		\
		PRINT_LOC_INFO;			\
		std::cout << "\t" << _tag;	\
		int _i=0;					\
		for(;_i<_vec.size(); _i++){			\
			std::cout << "\t" << _vec[_i]->number;	\
		}						\
		std::cout << std::endl;		                \
	} while(0)

#else	//HLSM_DEBUG_DUMP not defined

#define DEBUG_INFO(_str)
#define DEBUG_INFO2(_arg1, _arg2)
#define DEBUG_INFO3(_arg1, _arg2, _arg3)
#define DEBUG_META_VEC(_tag, _vec)
#define DEBUG_MEASURE(func_, tag_)

#endif  //HLSM_DEBUG_DUMP

#endif  //HLSM_DEBUG_H
