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

#ifndef HDFS_SCHEMA_H
#define	HDFS_SCHEMA_H

#include "hdfs.h"
#include <string>
#include <exception>

class HDFSSchema
{
public:
  HDFSSchema(const std::string& host, int port, const std::string& user,
                                        const std::string& parent_path);
  ~HDFSSchema();
  int HDFS_data_insert(const std::string& DPath, const char* data);

  std::string get_data_warehouse_dir()
  {
    return m_parent_path;
  }

  std::string hdfs_field_delim;
  std::string hdfs_row_delim;

private:
  hdfsFS m_fs;
  std::string m_host;
  std::string m_user;
  std::string m_parent_path;
  int m_port;
};

#endif  /* HDFS_SCHEMA_INCLUDED */
