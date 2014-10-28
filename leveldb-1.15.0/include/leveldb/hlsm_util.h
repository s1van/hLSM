#ifndef HLSM_UTIL_H
#define HLSM_UTIL_H

namespace hlsm {
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
			keypool[i] = hash(i);
		}
		sort(keypool,0,keysize*filenum);
	}
	~YCSBKeyGenerator() {
		delete keypool;
	}
	long hash(long long val);
	void sort(long *num, int top, int bottom);
	int partition(long *array, int top, int bottom);
	long nextKey();
};

} // hlsm

#endif
