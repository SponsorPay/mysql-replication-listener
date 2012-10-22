/*
  Copyright (c) 2012, Oracle and/or its affiliates. All rights
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
#include <iostream>
#include <iomanip>

/*
  @file binlog_browser

  This is a basic example that opens a binary log and prints out
  what events are found. It uses a simple event loop and prints the
  event types, length and start and end positions of the events.
*/

using namespace std;
using mysql::Binary_log;
using mysql::system::create_transport;
using mysql::system::get_event_type_str;

int main(int argc, char** argv)
{
  if (argc != 2)
  {
    cerr << "Usage: binlog_browser <uri>" << endl;
    exit(2);
  }

  Binary_log binlog(create_transport(argv[1]));

  if(binlog.connect() != ERR_OK)
  {
    cerr << "Unable to setup connection" << endl;
    exit(2);
  }

  cout << setw(17) << left
       << "Start Position"
       << setw(15) << left
       << "End Position"
       << setw(15) << left
       << "Event Length"
       << "Event Type"
       << endl;

  while(true)
  {
    Binary_log_event *event;
    const int result= binlog.wait_for_next_event(&event);
    int event_start_pos;

    if(result == ERR_EOF)
      break;

    event_start_pos= binlog.get_position()-(event->header())->event_length;

    cout << setw(17) << left
         << event_start_pos
         << setw(15) << left
         << (event->header())->next_position
         << setw(15) << left
         << (event->header())->event_length
         << get_event_type_str(event->get_event_type())
         << "[" << event->get_event_type() << "]"
         << endl;
  }
}

