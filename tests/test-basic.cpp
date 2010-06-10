#include "repevent.h"
#include <gtest/gtest.h>
#include <iostream>
#include <stdlib.h>

TEST(TestBinaryLog, CreateTransport_TcpIp) {
  MySQL::system::Binary_log_driver *drv= MySQL::create_transport("mysql://nosuchuser@128.0.0.1:99999");
  EXPECT_FALSE(drv == NULL);
  delete drv;
}

TEST(TestBinaryLog, CreateTransport_Bogus)
{
  MySQL::system::Binary_log_driver *drv= MySQL::create_transport("bogus-url");
  EXPECT_TRUE(drv == NULL);
  delete drv;
}

TEST(TestBinaryLog, ConnectTo_Bogus)
{
  MySQL::Binary_log *binlog= new MySQL::Binary_log(MySQL::create_transport("bogus-url"));
  EXPECT_GT(binlog->connect(), 0);
  delete(binlog);
}

TEST(TestBinaryLog, ConnectTo_TcpIp)
{
  MySQL::Binary_log *binlog= new MySQL::Binary_log(MySQL::create_transport("mysql://root@127.0.0.1:13000"));
  EXPECT_EQ(binlog->connect(),0);
  delete binlog;
}

TEST(TestBinaryLog, Connected_TcpIp)
{
  MySQL::Binary_log *binlog= new MySQL::Binary_log(MySQL::create_transport("mysql://root@127.0.0.1:13000"));
  EXPECT_EQ(binlog->connect(),0);
  MySQL::Binary_log_event_ptr event;
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

