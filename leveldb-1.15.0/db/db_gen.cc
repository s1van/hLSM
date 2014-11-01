#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/types.h>
#include <unistd.h>

#include "db/db_impl.h"
#include "db/hlsm_impl.h"
#include "db/version_set.h"
#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"
#include "leveldb/hlsm.h"
#include "port/port.h"
#include "util/crc32c.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/testutil.h"

// Number of key/values to place in database
static int FLAGS_num = 1000000;

// Size of each value
static int FLAGS_value_size = 100;
const int key_size = 20;

// Number of bytes to buffer in memtable before compacting
// (initialized to default value by "main")
static int FLAGS_write_buffer_size;

// Arrange to generate values that shrink to this fraction of
// their original size after compression
static double FLAGS_compression_ratio = 0;

// Bloom filter bits per key.
// Negative means use default settings.
static int FLAGS_bloom_bits = -1;

// Maximum number of files to keep open at the same time (use default if == 0)
static int FLAGS_open_files = 16000;

// Use the db with the following name.
static const char* FLAGS_db = NULL;

//key range of requests
static int64_t FLAGS_write_from = 0;
static int64_t FLAGS_write_upto = -1;
static int64_t FLAGS_write_span = -1;

static int FLAGS_cache_size = 131072;
static int FLAGS_random_seed = 301;
static int FLAGS_ycsb_compatible = 0;

// generate more files per level (than maximum number of files before triggering compaction)
//     to activate compaction at the beginning
static int FLAGS_extra_files_per_level = 0;
static int kv_pair_overhead_bytes = 30;

using namespace leveldb;

// Helper for quickly generating random data from db_bench.cc
class RandomGenerator {
 private:
  std::string data_;
  int pos_;

 public:
  RandomGenerator() {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < 1048576) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      test::CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }

  Slice Generate(size_t len) {
    if (pos_ + len > data_.size()) {
      pos_ = 0;
      assert(len < data_.size());
    }
    pos_ += len;
    return Slice(data_.data() + pos_ - len, len);
  }
};

class Generator {
private:
	Cache* cache_;
	const FilterPolicy* filter_policy_;
	DB* db_;
	int num_;
	int value_size_;

	Random rand_;
	WriteOptions write_options_;
	int entries_per_batch_;

  Options options_;

public:
	Generator()
: cache_(FLAGS_cache_size >= 0 ? NewLRUCache(FLAGS_cache_size) : NULL),
  filter_policy_(FLAGS_bloom_bits >= 0
  		? NewBloomFilterPolicy(FLAGS_bloom_bits)
  				: NULL),
  				  db_(NULL),
  				  num_(FLAGS_num),
  				  value_size_(FLAGS_value_size),
  				  entries_per_batch_(64),
  				  rand_(FLAGS_random_seed),
  				  write_options_(WriteOptions()){
		std::vector<std::string> files;
		Env::Default()->GetChildren(FLAGS_db, &files);
		for (size_t i = 0; i < files.size(); i++) {
			if (Slice(files[i]).starts_with("heap-")) {
				Env::Default()->DeleteFile(std::string(FLAGS_db) + "/" + files[i]);
			}
		}

		DestroyDB(FLAGS_db, Options());
		hlsm::config::run_compaction = 0; // no compaction
	}

	~Generator() {
		delete db_;
		delete cache_;
		delete filter_policy_;
		fprintf(stdout, "DB generation completes\n");
	}

  void Open() {
    assert(db_ == NULL);
    options_.create_if_missing = true;
    options_.block_cache = cache_;
    options_.write_buffer_size = FLAGS_write_buffer_size;
    options_.max_open_files = FLAGS_open_files;
    options_.filter_policy = filter_policy_;
    options_.compression = leveldb::kNoCompression;
    Status s = DB::Open(options_, FLAGS_db, &db_);
    if (!s.ok()) {
      fprintf(stderr, "open error: %s\n", s.ToString().c_str());
      exit(1);
    }
  }

	int Run() {
		Open();

		RandomGenerator gen;
		WriteBatch batch;
		batch.Clear();
		Status s;
		hlsm::YCSBKeyGenerator *ycsb_gen;

		int done = 0, bnum = 0;
		char key[100];

		//skip level 0
		int level = 0;
		int clevel_max_fnum = 0;
		int clevel_fnum = clevel_max_fnum;
		int64_t file_key_start, file_key_span;

		for (int i = 0; done < FLAGS_num; i++) {
			// current level is finished, move to next level
			if (clevel_max_fnum == 0 || clevel_fnum >= clevel_max_fnum + FLAGS_extra_files_per_level) {
				if (level % 2 == 1) {
					reinterpret_cast<DBImpl*>(db_)->AdvanceHLSMActiveDeltaLevel(level);
				}
				level++;
				clevel_max_fnum = hlsm::max_fnum_in_level(level);
				clevel_fnum = 0;

				// reinitialize ycsb generator for the new level
				if (FLAGS_ycsb_compatible) {
					ycsb_gen = new hlsm::YCSBKeyGenerator(i, clevel_max_fnum + FLAGS_extra_files_per_level,
							(int) leveldb::config::kTargetFileSize/ (FLAGS_value_size + kv_pair_overhead_bytes));
					DEBUG_INFO(2, "New level %d starts with %d (YCSBKeyGen)\n", level, i);
				}

				file_key_span = (int) (FLAGS_write_span / (clevel_max_fnum + FLAGS_extra_files_per_level));
				file_key_start = FLAGS_write_from;
				DEBUG_INFO(2, "start: %ld, span: %ld, fnum_max: %d, level: %d\n",
						file_key_start, file_key_span, clevel_max_fnum, level);
			}

			if (FLAGS_ycsb_compatible) {
				snprintf(key, sizeof(key), "user%019lld", ycsb_gen->nextKey());
			} else {
				const uint64_t k = file_key_start + (rand_.Next64() % file_key_span);
				snprintf(key, sizeof(key), "%020lu", k);
			}
			DEBUG_INFO(4, "Insert key %s (k = %d)\n", key, i);
			batch.Put(key, gen.Generate(value_size_));

			done++;
			bnum++;
			if (bnum == entries_per_batch_) {
				bnum = 0;
				fprintf(stderr, "... almost finished %d ops%30s\r", done, "");
				DEBUG_MEASURE_RECORD(2, (s = db_->Write(write_options_, &batch)), "RW--Write");
				fprintf(stderr, "... finished %d ops%30s\r", done, "");
				batch.Clear();
				if (!s.ok()) {
					fprintf(stderr, "put error: %s\n", s.ToString().c_str());
					exit(1);
				}

				// write one immutable table, move to next file
				if (reinterpret_cast<DBImpl*>(db_)->MaybeCompactMemTableToLevel(level) == 1) {
					clevel_fnum++;
					if (!FLAGS_ycsb_compatible) {
						file_key_start += file_key_span;
						DEBUG_INFO(2, "start: %ld, span: %ld, fnum_max: %d, level: %d, fnum: %d\n",
												file_key_start, file_key_span, clevel_max_fnum, level, clevel_fnum);
					}
				}
			}
		}

		// advance the delta level of the last logical level
		reinterpret_cast<DBImpl*>(db_)->AdvanceHLSMActiveDeltaLevel(level);

		return 0;
	}

};


int main(int argc, char** argv) {
  std::string default_db_path;

  for (int i = 1; i < argc; i++) {
    double d;
    int n;
    int64_t n64;
    char junk;
    if (sscanf(argv[i], "--compression_ratio=%lf%c", &d, &junk) == 1) {
      FLAGS_compression_ratio = d;
    } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
      FLAGS_num = n;
    } else if (sscanf(argv[i], "--write_key_from=%ld%c", &n64, &junk) == 1) {
    	FLAGS_write_from = n64;
    } else if (sscanf(argv[i], "--write_key_upto=%ld%c", &n64, &junk) == 1) {
    	FLAGS_write_upto = n64;
    } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
      FLAGS_value_size = n;
    } else if (sscanf(argv[i], "--write_buffer_size=%d%c", &n, &junk) == 1) {
      FLAGS_write_buffer_size = n;
    } else if (sscanf(argv[i], "--bloom_bits=%d%c", &n, &junk) == 1) {
      FLAGS_bloom_bits = n;
    } else if (sscanf(argv[i], "--bloom_bits_use=%d%c", &n, &junk) == 1) {
      hlsm::config::bloom_bits_use = n;
    } else if (sscanf(argv[i], "--open_files=%d%c", &n, &junk) == 1) {
      FLAGS_open_files = n;
    } else if (strncmp(argv[i], "--db=", 5) == 0) {
      FLAGS_db = argv[i] + 5;
      hlsm::config::primary_storage_path = FLAGS_db;
    }  else if (strncmp(argv[i], "--hlsm_mode=", 12) == 0) {
      hlsm::config::mode.set(argv[i] + 12);
    } else if (sscanf(argv[i], "--hlsm_cursor_compaction=%d%c", &n, &junk) == 1) {
      hlsm::runtime::use_cursor_compaction = n;
    } else if (strncmp(argv[i], "--hlsm_secondary_storage_path=", 30) == 0) {
      hlsm::config::secondary_storage_path = argv[i] + 30;
    } else if (sscanf(argv[i], "--file_size=%d%c", &n, &junk) == 1) {
      leveldb::config::kTargetFileSize = n * 1048576; // in MiB
    } else if (sscanf(argv[i], "--random_seed=%lf%c", &d, &junk) == 1) {
      FLAGS_random_seed = d;
    } else if (sscanf(argv[i], "--ycsb_compatible=%lf%c", &d, &junk) == 1) {
      FLAGS_ycsb_compatible = d;
    } else if (sscanf(argv[i], "--level0_size=%d%c", &n, &junk) == 1) {
      leveldb::config::kL0_Size = n;
    } else if (sscanf(argv[i], "--level_ratio=%d%c", &n, &junk) == 1) {
      leveldb::config::kLevelRatio = n;
    } else if (sscanf(argv[i], "--debug_level=%d%c", &n, &junk) == 1) {
      hlsm::config::debug_level = n;
    } else if (sscanf(argv[i], "--run_compaction=%d%c", &n, &junk) == 1) {
      hlsm::config::run_compaction = n;
    } else if (sscanf(argv[i], "--extra_files_per_level=%d%c", &n, &junk) == 1) {
    	FLAGS_extra_files_per_level = n;
    } else if (strncmp(argv[i], "--debug_file=", 13) == 0) {
      hlsm::config::debug_file = argv[i] + 13;
    } else {
      fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
      exit(1);
    }
  }

  if (FLAGS_write_upto == -1) {
  	FLAGS_write_upto = FLAGS_num;
  	FLAGS_write_span = FLAGS_num;
  }

  FLAGS_write_span = FLAGS_write_upto - FLAGS_write_from;
  fprintf(stderr, "Range: %ld(w)\n", FLAGS_write_span);
  FLAGS_write_buffer_size = leveldb::config::kTargetFileSize;


  // Choose a location for the test database if none given with --db=<path>
  if (FLAGS_db == NULL) {
      default_db_path = "/tmp/db_gen";
      FLAGS_db = default_db_path.c_str();
  }

  Generator Gen;
  Gen.Run();
  return 0;
}
