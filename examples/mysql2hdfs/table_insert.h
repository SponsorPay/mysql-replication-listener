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

#ifndef TABLE_INSERT_INCLUDED
#define	TABLE_INSERT_INCLUDED

#include "binlog_api.h"
#include "hdfs_schema.h"
#include <string>
#include <map>

extern std::vector<long int> field_index;
extern std::vector<std::string> opt_db_name;

typedef std::vector<std::string> tname_vec;
typedef std::pair<std::string, tname_vec> opt_dname_element;

extern std::map<std::string, tname_vec> dbname2tbname_map;

extern long int opt_field_max;
extern long int opt_field_min;
extern int opt_field_flag;
extern int opt_db_flag;

void table_insert(const std::string& db_name, const std::string& table_name,
                  const mysql::Row_of_fields &fields,
                  long int timestamp, HDFSSchema *mysqltohdfs_obj);

#endif	/* TABLE_INSERT_INCLUDED */
