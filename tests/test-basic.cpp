#include "repevent.h"
#include <gtest/gtest.h>

class TestBinaryLog : public testing::TestWithParam<const char*> {
public:
  TestBinaryLog()
  : binlog(NULL)
  {
  }

protected:
  MySQL::Binary_log *binlog;
};

TEST_P(TestBinaryLog, CheckNull) {
  // This is not really a very good test. It should be replaced with a
  // test that the creation of a Binary_log instance worked OK.
  EXPECT_TRUE(binlog == NULL);
}

// Here are some tentative tests
#if 0
TEST_P(TestBinaryLog, ReadEmptyBinlog) {
  // Test that a binlog only holding a Format_description event and a
  // Rotate event can be read correctly.
}
#endif

INSTANTIATE_TEST_CASE_P(UrlTesting, TestBinaryLog,
                        ::testing::Values("file:binlog-master.000001"));

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
