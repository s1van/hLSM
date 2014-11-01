#ifndef HLSM_UTIL_H
#define HLSM_UTIL_H

#include <time.h>
#include <stdlib.h>

namespace hlsm {

static inline long long YCSBKey_hash(long long val)
{
  	long long FNV_offset_basis_64=0xCBF29CE484222325LL;
  	long long FNV_prime_64=1099511628211LL;
  	long long hashval = FNV_offset_basis_64;
  	for (int i=0; i<8; i++)
  	{
  		long long octet=val&0x00ff;
  		val=val>>8;
  		hashval = hashval ^ octet;
  		hashval = hashval * FNV_prime_64;
  	}
  	return llabs(hashval);
}

class YCSBKeyGenerator{
private:
	long long *keypool;
	int index;
	int max;

public:
	YCSBKeyGenerator(int startnum, int filenum, int keysize):index(0) {
		keypool = new long long[keysize*filenum];
		for(long long i=0;i<keysize*filenum;i++)
		{
			keypool[i] = YCSBKey_hash(i + startnum);
		}
		sort(keypool,0,keysize*filenum);
	}
	~YCSBKeyGenerator() {
		delete keypool;
	}
	void sort(long long *num, int top, int bottom);
	int partition(long long *array, int top, int bottom);
	long long nextKey();
};

} // hlsm

#endif
