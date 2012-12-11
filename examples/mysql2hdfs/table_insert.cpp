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

#include "table_insert.h"
#include "hdfs_schema.cpp"
#include <stdlib.h>
#include <string>

void table_insert(std::string table_name, mysql::Row_of_fields &fields,
                  long int timestamp, HDFSSchema *sqltohdfs_obj)
{
  mysql::Row_of_fields::iterator field_it= fields.begin();

  mysql::Converter converter;
  int col= 0;

  /*
    Save the presumed table key for later use, may be useful for update
    operations.
  */

  std::string rowid;
  converter.to(rowid, *field_it);
  std::ostringstream data;
  data << timestamp;

  do
  {
    /*
      Each row contains a vector of Value objects. The converter
      allows us to transform the value into another
      representation.
    */
    data << sqltohdfs_obj->hdfs_field_delim;
    std::string str;
    converter.to(str, *field_it);
    data << str;
  } while (++field_it != fields.end());

  data << sqltohdfs_obj->hdfs_row_delim;

  /* Call the function to insert data into HDFS */
  sqltohdfs_obj->HDFS_data_insert(table_name, data.str().c_str());
}
