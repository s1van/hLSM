#include "util/testharness.h"
#include "db/lazy_version_edit.h"

using namespace leveldb;
namespace hlsm {

/*
 * LazyVersionEdit
 */

static void TestEncodeDecode(const LazyVersionEdit& edit) {
  std::string encoded, encoded2;
  edit.EncodeTo(&encoded);
  LazyVersionEdit parsed;
  Status s = parsed.DecodeFrom(encoded);
  ASSERT_TRUE(s.ok()) << s.ToString();
  parsed.EncodeTo(&encoded2);
  ASSERT_EQ(encoded, encoded2);
}

class VersionEditTest { };

TEST(VersionEditTest, EncodeDecode) {
  static const uint64_t kBig = 1ull << 50;

  LazyVersionEdit edit;
  for (int i = 0; i < 4; i++) {
    TestEncodeDecode(edit);
    edit.AddFile(3, kBig + 300 + i, kBig + 400 + i,
                 InternalKey("foo", kBig + 500 + i, kTypeValue),
                 InternalKey("zoo", kBig + 600 + i, kTypeDeletion));
    edit.AddLazyFile(5, kBig + 300 + i, kBig + 400 + i,
                 InternalKey("foo", kBig + 500 + i, kTypeValue),
                 InternalKey("zoo", kBig + 600 + i, kTypeDeletion));
    edit.DeleteFile(4, kBig + 700 + i);
    edit.DeleteLazyFile(6, kBig + 700 + i);
    edit.SetCompactPointer(i, InternalKey("x", kBig + 900 + i, kTypeValue));
  }

  edit.SetComparatorName("foo");
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);
  TestEncodeDecode(edit);
}


/*
 * LazyVersionSet
 */

}  // namespace hlsm

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
