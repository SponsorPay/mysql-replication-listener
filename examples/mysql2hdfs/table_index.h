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

#ifndef TABLE_INDEX_INCLUDED
#define	TABLE_INDEX_INCLUDED

#include "binlog_event.h"
#include "basic_content_handler.h"
#include <map>

typedef std::pair<long int, mysql::Table_map_event *> Event_index_element;
typedef std::map<long int, mysql::Table_map_event *> Int2event_map;

class Table_index : public mysql::Content_handler, public Int2event_map
{
public:
  mysql::Binary_log_event *process_event(mysql::Table_map_event *tm);

  ~Table_index();
};

#endif	/* TABLE_INDEX_INCLUDED */
