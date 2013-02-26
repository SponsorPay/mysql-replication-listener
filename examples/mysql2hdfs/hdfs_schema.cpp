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

/*
   A table is mapped as a file into a directory in HDFS. The data to be
   inserted in a table are files with a .txt extension. Databases are
   represented as separate directories.

   A directory in HDFS file system is created in the following events:

   1. The table has not been created earlier, and hence target directory
       does not exist.
   2. Looking for a directory in the file structure and the directory
      cannot be found.

   TODO: No code for handling partitions in the table

   Whenever data is to be inserted in a table, a file in the folder with the
   name of the table is created/appended with the new data. Files are created
   only in two cases:

   1. The table is initially empty.

   2. The datafile size increases to its maximum value.

   TODO: The maximum file size has not been specified. I am not sure if its
         required in the first place.
*/

#include "table_index.h"
#include "binlog_api.h"
#include "hdfs_schema.h"
#include <iostream>
#include <stdexcept>

using namespace std;


/**
  The constructor connects to HDFS File system.
  @param host           A string containing either a host name, or an IP address
                        of the namenode of a hdfs cluster. 'host' should be passed as
                        NULL if you want to connect to local filesystem. 'host' should
                        be passed as 'default' (and port as 0) to used the 'configured'
                        filesystem (core-site/core-default.xml).
  @param port           The port on which the server is listening.
  @param user           The user name (this is hadoop domain user)
  @param parent_path    Data warehouse directory to build the database schema.
*/

HDFSSchema::HDFSSchema(const std::string& host, int port,
                       const std::string& user,
                       const std::string& parent_path)
 : m_user(user), m_parent_path(parent_path)
{
  /* Connect to HDFS File system */
  if (user.empty())
    m_fs= hdfsConnect(host.c_str(), port);

  else
    m_fs= hdfsConnectAsUser(host.c_str(), port, user.c_str());

  m_host= host;
  m_port= port;

  /* The default data  warehouse diectory is set. */
  if (m_parent_path.empty())
    m_parent_path= "/user/hive/warehouse";

  if (m_fs == NULL)
  {
    std::runtime_error error("Couldnot connect to HDFS file system");
    throw error;
  }
  else
    cout << "Connected to HDFS file system" << endl;

}

/* Disconnect from hdfs. */
HDFSSchema::~HDFSSchema()
{
  if (m_fs)
    hdfsDisconnect(m_fs);
}

/**
  This function is called internally from table_insert function.
  It recieves the field data, and inserts it into a file in the
  target directory path in the HDFS file system.

  @param DPath  It is the target directory path, which has the format
                db_name/tb_name.
  @param data   Contains the field value for one row inserted in the table
  @return       Returns 1 if successful or 0 on error

*/

int HDFSSchema::HDFS_data_insert(const string& DPath, const char* data)
{
  std::stringstream stream_dir_path;
  stream_dir_path << "hdfs://" << m_host << ":" << m_port << m_parent_path << "/";
  stream_dir_path << DPath;

  if (hdfsSetWorkingDirectory(m_fs, (stream_dir_path.str()).c_str()))
  {
    cerr << "Failed to set working directory as " << stream_dir_path.str();
    return 0;
  }
  const char* write_path= "datafile1.txt";
  hdfsFile writeFile;

  /*
    The file in which the data is stored is named datafile1.txt here;
    you can name it anything you want. The working directory where this
    datafile goes is hive_warehouse/db_name.db/tb_name.
    There is no restriction on the maximum size of the file.
    should there be?
  */

  if (!hdfsExists(m_fs, write_path))  //The path exists
  {
    /* Append data to the datafile */
    writeFile= hdfsOpenFile(m_fs, write_path, O_WRONLY|O_APPEND, 0, 0, 0);
  }
  else
  {
    /* File doesnot exist, create a new one. */
    writeFile = hdfsOpenFile(m_fs, write_path, O_WRONLY|O_CREAT, 0, 0, 0);
  }

  if (!writeFile)
  {
    cerr << "Failed to open " << write_path << " for writing!\n";
    return 0;
  }

  tSize num_written_bytes = hdfsWrite(m_fs, writeFile, (void*)data, strlen(data));
  cout << "Written "
       << num_written_bytes
       << " bytes to datafile in the following directory: "
       << stream_dir_path.str()
       << endl;
  if (hdfsFlush(m_fs, writeFile))
  {
    cerr <<  "Failed to 'flush' " << write_path << "\n";
    return 0;
  }
  hdfsCloseFile(m_fs, writeFile);
  return 1;
} // end HDFS_data_insert
