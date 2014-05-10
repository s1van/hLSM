namespace leveldb {

namespace config {
int kTargetFileSize = 2 * 1048576;
int kL0_Size = 10;       // in MB
int kLevelRatio = 10;    // enlarge the level size ten times when the db levels up
}

}

namespace hlsm {

namespace config {
int full_mirror = 0;
const char *secondary_storage_path;
}

}
