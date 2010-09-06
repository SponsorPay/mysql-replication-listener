#include "binlog_api.h"
#include <gtest/gtest.h>
#include <iostream>
#include <stdlib.h>
class TestBinaryLog : public ::testing::Test {
 protected:
  TestBinaryLog() {
    // You can do set-up work for each test here.
  }

  virtual ~TestBinaryLog() {
    // You can do clean-up work that doesn't throw exceptions here.
  }

  // If the constructor and destructor are not enough for setting up
  // and cleaning up each test, you can define the following methods:

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }

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

TEST_F(TestBinaryLog, ConnectTo_TcpIp)
{
  MySQL::Binary_log *binlog= new MySQL::Binary_log(MySQL::create_transport("mysql://root@127.0.0.1:13000"));
  EXPECT_EQ(binlog->connect(),0);
  delete binlog;
}

TEST_F(TestBinaryLog, Connected_TcpIp)
{
  MySQL::Binary_log *binlog= new MySQL::Binary_log(MySQL::create_transport("mysql://root@127.0.0.1:13000"));
  EXPECT_EQ(binlog->connect(),0);
  MySQL::Binary_log_event *event;
  binlog->wait_for_next_event(event);
  EXPECT_TRUE(event->get_event_type() == MySQL::ROTATE_EVENT);
  delete event;
  binlog->wait_for_next_event(event);
  EXPECT_TRUE(event->get_event_type() == MySQL::FORMAT_DESCRIPTION_EVENT);
  delete event;
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  // TODO require that the connection string is passed as an argument to the
  // test suite.
  std::cout << "Important: Make sure that the MySQL server is started using "
    "'mysql-test-run --mysqld=--log_bin --mysqld=--binlog_format=row --start "
    "alias' and that the server is listening on IP 127.0.0.1 and port"
    " 13000." << std::endl;
  return RUN_ALL_TESTS();
}

