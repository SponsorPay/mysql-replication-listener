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

#include "table_insert.h"
#include "hdfs_schema.h"
#include <stdlib.h>

long int opt_field_max= 0;
long int opt_field_min= 0;
int opt_field_flag= 0;
int opt_db_flag= 0;
void table_insert(const std::string& db_name, const std::string& table_name,
                  const mysql::Row_of_fields &fields,
                  long int timestamp, HDFSSchema *sqltohdfs_obj)
{
  mysql::Row_of_fields::const_iterator field_it= fields.begin();

  mysql::Converter converter;
  int col= 0;
  int field_index_counter=0;

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
    field_index_counter++;
    std::vector<long int>::iterator it;
    if (opt_field_flag)
    {
      if (!field_index.empty())
        it= find(field_index.begin(), field_index.end(), field_index_counter);
      if ((field_index.empty() || *it == 0) &&
          (opt_field_min == 0 || field_index_counter < opt_field_min) &&
          (opt_field_max == 0 || field_index_counter > opt_field_max))
        continue;
    }

    std::string str;
    converter.to(str, *field_it);

    data << sqltohdfs_obj->hdfs_field_delim;
    data << str;

  } while (++field_it != fields.end());

  data << sqltohdfs_obj->hdfs_row_delim;

  if (opt_db_flag && !dbname2tbname_map.empty())
  {
    std::map<std::string, tname_vec>::iterator db_it=
                                      dbname2tbname_map.begin();
    db_it= dbname2tbname_map.find(db_name);
    if (db_it == dbname2tbname_map.end())
    {
      std::cout << "Skipping queries on database "
                << db_name
                << std::endl;
      return;
    }
    if (!((*db_it).second).empty())
    {
      if (find((*db_it).second.begin(),
               (*db_it).second.end(), table_name) == (*db_it).second.end())
      {
        std::cout << "Skipping queries on table "
                  << table_name << " in the database "
                  << db_name << std::endl;
        return;
      }
    }
  }

  /* Create a fuly qualified table name */
  std::ostringstream os;
  os << db_name << ".db/" << table_name;
  if (sqltohdfs_obj != NULL)
  {
    /* Call the function to insert data into HDFS */
    sqltohdfs_obj->HDFS_data_insert(os.str().c_str(), data.str().c_str());
  }
}
