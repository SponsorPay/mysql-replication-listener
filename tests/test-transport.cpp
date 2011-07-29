/*
Copyright (c) 2003, 2011, Oracle and/or its affiliates. All rights
reserved.

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; version 2 of
the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
02110-1301  USA
*/

#include "binlog_api.h"
#include <gtest/gtest.h>
#include <iostream>
#include <stdlib.h>

using mysql::system::create_transport;
using mysql::system::Binary_log_driver;
using mysql::system::Binlog_tcp_driver;
using mysql::system::Binlog_file_driver;

class TestTransport : public ::testing::Test {
protected:
  TestTransport() { }
  virtual ~TestTransport() { }
};

void CheckTcpValues(Binlog_tcp_driver *tcp,
                    const char *user, const char *passwd,
                    const char *host, unsigned long port)
{
  EXPECT_EQ(tcp->port(), port);
  EXPECT_EQ(tcp->host(), host);
  EXPECT_EQ(tcp->user(), user);
  EXPECT_EQ(tcp->password(), passwd);
}


void TestTcpTransport(const char *uri,
                      const char *user, const char *passwd,
                      const char *host, unsigned long port)
{
  Binary_log_driver *drv= create_transport(uri);
  EXPECT_TRUE(drv);
  Binlog_tcp_driver* tcp = dynamic_cast<Binlog_tcp_driver*>(drv);
  EXPECT_TRUE(tcp);
  CheckTcpValues(tcp, user, passwd, host, port);
  delete drv;
}


TEST_F(TestTransport, CreateTransport_TcpIp) {
  TestTcpTransport("mysql://nosuchuser@128.0.0.1:99999",
                   "nosuchuser", "", "128.0.0.1", 99999);
  TestTcpTransport("mysql://nosuchuser@128.0.0.1:3306",
                   "nosuchuser", "", "128.0.0.1", 3306);
  TestTcpTransport("mysql://nosuchuser:magic@128.0.0.1:3306",
                   "nosuchuser", "magic", "128.0.0.1", 3306);
  TestTcpTransport("mysql://nosuchuser:magic@example.com:3306",
                   "nosuchuser", "magic", "example.com", 3306);
  EXPECT_FALSE(create_transport("mysql://nosuchuser@128.0.0.1"));
  EXPECT_FALSE(create_transport("mysql://:magic@128.0.0.1:99999"));
  EXPECT_FALSE(create_transport("mysql://nosuchuser@:99999"));
  EXPECT_FALSE(create_transport("mysql://@128.0.0.1:99999"));
}

TEST_F(TestTransport, CreateTransport_File) {
  Binary_log_driver *drv= create_transport("file://master-bin.000003");
  EXPECT_TRUE(drv);
  EXPECT_TRUE(dynamic_cast<Binlog_file_driver*>(drv));
  delete drv;
}

TEST_F(TestTransport, CreateTransport_Bogus)
{
  EXPECT_FALSE(create_transport("bogus-url"));
  EXPECT_FALSE(create_transport("fil://almost-correct.txt"));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
