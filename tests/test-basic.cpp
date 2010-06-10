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


TEST_F(TestBinaryLog, CreateTransport_TcpIp) {
  MySQL::system::Binary_log_driver *drv= MySQL::create_transport("mysql://nosuchuser@128.0.0.1:99999");
  EXPECT_FALSE(drv == NULL);
  delete drv;
}

TEST_F(TestBinaryLog, CreateTransport_Bogus)
{
  MySQL::system::Binary_log_driver *drv= MySQL::create_transport("bogus-url");
  EXPECT_TRUE(drv == NULL);
  delete drv;
}

TEST_F(TestBinaryLog, ConnectTo_Bogus)
{
  MySQL::Binary_log *binlog= new MySQL::Binary_log(MySQL::create_transport("bogus-url"));
  EXPECT_GT(binlog->connect(), 0);
  delete(binlog);
}

// Here are some tentative tests
#if 0
TEST_P(TestBinaryLog, ReadEmptyBinlog) {
  // Test that a binlog only holding a Format_description event and a
  // Rotate event can be read correctly.
}
#endif

//INSTANTIATE_TEST_CASE_P(UrlTesting, TestBinaryLog,
//                        ::testing::Values("file:binlog-master.000001"));
//
//int main(int argc, char *argv[]) {
//  ::testing::InitGoogleTest(&argc, argv);
//  return RUN_ALL_TESTS();
//}
