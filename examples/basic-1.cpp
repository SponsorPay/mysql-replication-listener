/*
Copyright (c) 2013, Oracle and/or its affiliates. All rights
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

/**
  @file basic-1
  @author Mats Kindahl <mats.kindahl@oracle.com>

  This is a basic example that just opens a binary log either from a
  file or a server and print out what events are found.  It uses a
  simple event loop and checks information in the events using a
  switch.
 */

using mysql::Binary_log;
using mysql::system::create_transport;

int main(int argc, char** argv) {

  if (argc != 2) {
    std::cerr << "Usage: basic-2 <uri>" << std::endl;
    exit(2);
  }

  Binary_log binlog(create_transport(argv[1]));
  binlog.connect();

  Binary_log_event *event;

  while (true) {
    int result = binlog.wait_for_next_event(&event);
    if (result != ERR_OK)
      break;
    std::cout << "Found event of type "
              << event->get_event_type()
              << std::endl;
  }
}
