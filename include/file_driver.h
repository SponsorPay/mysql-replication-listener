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

#ifndef _FILE_DRIVER_H
#define	_FILE_DRIVER_H

#include <iostream>
#include <fstream>

#include "binlog_api.h"
#include "binlog_driver.h"
#include "protocol.h"


namespace mysql { namespace system {

class Binlog_file_driver : public Binary_log_driver
{
public:

    Binlog_file_driver(const std::string& binlog_file_name)
    {
        m_binlog_file_name= binlog_file_name;
    }

    ~Binlog_file_driver()
    {
    }

    int connect();
    int disconnect();
    int wait_for_next_event(mysql::Binary_log_event **event);
    int set_position(const std::string &str, unsigned long position);
    int get_position(std::string *str, unsigned long *position);
    Binary_log_event* parse_event();

private:

    unsigned long m_binlog_file_size;
    unsigned long m_binlog_offset;

    /*
      Bytes that has been read so for from the file.
      Updated after every event is read.
    */
    unsigned long m_bytes_read;

    /*
      Used while reading relay logs, where events
      may appear after rotate event. Its usually
      4 to correct the magic bytes as they are not
      present in logs after rotate event but the
      next_position do consider them.
    */
    unsigned long m_correction_bytes;

    /*
      Number of bytes read before the next
      format_description_event is encountered.
    */
    unsigned long m_size_before_desc_event;
    std::string m_binlog_file_name;
    std::ifstream m_binlog_file;

    Log_event_header m_event_log_header;
};

}
}

#endif	/* _FILE_DRIVER_H */
