#ifndef HLSM_LAZY_VERSION_SET_H
#define HLSM_LAZY_VERSION_SET_H

#include "db/version_set.h"
#include "db/lazy_version_edit.h"
#include "leveldb/hlsm_param.h"

namespace leveldb {

class LazyVersionSet: public VersionSet {

public:
	LazyVersionSet(const std::string& dbname,
			const Options* options,
			TableCache* table_cache,
			const InternalKeyComparator*);
	~LazyVersionSet();

	Status LogAndApply(VersionEdit* edit, port::Mutex* mu)
		EXCLUSIVE_LOCKS_REQUIRED(mu);

	Status Recover();
	void AddLiveLazyFiles(std::set<uint64_t>* live);
	Status MoveLevelDown(leveldb::Compaction* c, leveldb::port::Mutex *mutex_);
	Status MoveFileDown(leveldb::Compaction* c, leveldb::port::Mutex *mutex_);

	Compaction* PickCompaction();
	void PrintVersionSet();

	hlsm::delta_meta_t* GetDeltaLevelOffsets() { return delta_meta_;  }
	Version* current_lazy() const { return current_lazy_; }
	bool isLazyLevelNonEmpty(int level) {return current_lazy_->NumFiles(level) > 0;}

private:
 friend class Compaction;
 friend class Version;
 friend class Builder;

 // Save current contents to *log
 Status WriteSnapshot(log::Writer* log);

 void AppendVersion(Version* v, Version* lv);


 Version dummy_lazy_versions_;
 Version* current_lazy_;

 hlsm::delta_meta_t delta_meta_[hlsm::runtime::kLogicalLevels]; // new level - offset returns the current(active) delta level

 // No copying allowed
 LazyVersionSet(const LazyVersionSet&);
 void operator=(const LazyVersionSet&);
};

} // namespace leveldb


#endif
