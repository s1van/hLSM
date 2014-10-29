#ifndef HLSM_UTIL_H
#define HLSM_UTIL_H

namespace hlsm {

static long YCSBKey_hash(long long val);

class YCSBKeyGenerator{
private:
	long *keypool;
	int index;
	int max;
public:
	YCSBKeyGenerator(int startnum, int filenum, int keysize):index(0) {
		keypool = new long[keysize*filenum];
		for(long long i=0;i<keysize*filenum;i++)
		{
			keypool[i] = YCSBKey_hash(i);
		}
		sort(keypool,0,keysize*filenum);
	}
	~YCSBKeyGenerator() {
		delete keypool;
	}
	void sort(long *num, int top, int bottom);
	int partition(long *array, int top, int bottom);
	long nextKey();
};



} // hlsm

#endif
