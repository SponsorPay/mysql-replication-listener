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

#include "file_driver.h"

namespace mysql { namespace system {

using namespace std;


  int Binlog_file_driver::connect()
  {

    char magic[]= {0xfe, 0x62, 0x69, 0x6e, 0};
    char *magic_buf= new char[5];

    m_binlog_file.exceptions(ifstream::failbit | ifstream::badbit |
                           ifstream::eofbit);

    try
    {

      // Calculate file size.
      m_binlog_file.open(m_binlog_file_name.c_str(), ios::in | ios::binary |
                         ios::ate);
      if (m_binlog_file.is_open())
      {
        m_binlog_file_size= m_binlog_file.tellg();
        m_binlog_file.close();
      }

      // Check if the file can be opened for reading.
      m_binlog_file.open(m_binlog_file_name.c_str(), ios::in | ios::binary);

      // Check if a valid MySQL binlog file is provided, BINLOG_MAGIC.
      m_binlog_file.read(magic_buf, 4);
      *(magic_buf+4)= 0;

      if(strcmp(magic, magic_buf))
      {
        return ERR_FAIL;                        // Not a valid binlog file.
      }

      // Reset the get pointer.
      m_binlog_file.seekg(0, ios::beg );

    } catch (...)
    {
      return ERR_FAIL;
    }
    delete[] magic_buf;

    return ERR_OK;
  }


  int Binlog_file_driver::disconnect()
  {
    m_binlog_file.close();
    return ERR_OK;
  }


  int Binlog_file_driver::set_position(const string &str, unsigned long position)
  {
    m_binlog_file.exceptions(ifstream::failbit | ifstream::badbit |
                           ifstream::eofbit);
    try
    {
      m_binlog_file.seekg(position, ios::beg );
    } catch(...)
    {
      return ERR_FAIL;
    }

    return ERR_OK;
  }


  int Binlog_file_driver::get_position(string *str, unsigned long *position)
  {
    m_binlog_file.exceptions(ifstream::failbit | ifstream::badbit |
                           ifstream::eofbit);
    try
    {
      if(position)
      {
        *position= m_binlog_file.tellg();
      }
    } catch(...)
    {
      return ERR_FAIL;
    }

    return ERR_OK;
  }


  int Binlog_file_driver::wait_for_next_event(mysql::Binary_log_event **event)
  {
    //TODO : Check for the valid position (atleast 4).

    m_binlog_file.exceptions(ifstream::failbit | ifstream::badbit |
                             ifstream::eofbit);

    try
    {
      if(m_bytes_read < m_binlog_file_size && m_binlog_file.good())
      {
        //Protocol_chunk<boost::uint8_t> prot_marker(m_event_log_header.marker);
        Protocol_chunk<boost::uint32_t> prot_timestamp(m_event_log_header.timestamp);
        Protocol_chunk<boost::uint8_t> prot_type_code(m_event_log_header.type_code);
        Protocol_chunk<boost::uint32_t> prot_server_id(m_event_log_header.server_id);
        Protocol_chunk<boost::uint32_t>
          prot_event_length(m_event_log_header.event_length);
        Protocol_chunk<boost::uint32_t>
          prot_next_position(m_event_log_header.next_position);
        Protocol_chunk<boost::uint16_t> prot_flags(m_event_log_header.flags);

        m_binlog_file >> prot_timestamp
                      >> prot_type_code
                      >> prot_server_id
                      >> prot_event_length
                      >> prot_next_position
                      >> prot_flags;

        /*
        m_binlog_file.read(reinterpret_cast<char*>(&m_event_log_header.timestamp),
                           sizeof(boost::uint32_t));
        m_binlog_file.read(reinterpret_cast<char*>(&m_event_log_header.type_code),
                           sizeof(boost::uint8_t));
        m_binlog_file.read(reinterpret_cast<char*>(&m_event_log_header.server_id),
                           sizeof(boost::uint32_t));
        m_binlog_file.read(reinterpret_cast<char*>(&m_event_log_header.event_length),
                           sizeof(boost::uint32_t));
        m_binlog_file.read(reinterpret_cast<char*>(&m_event_log_header.next_position),
                           sizeof(boost::uint32_t));
        m_binlog_file.read(reinterpret_cast<char*>(&m_event_log_header.flags),
                           sizeof(boost::uint16_t));
                           */

        *event= parse_event();
        m_bytes_read= m_binlog_file.tellg();

        if(*event)
          return ERR_OK;
      }
    } catch(...)
    {
      return ERR_FAIL;
    }
    return ERR_EOF;
  }


  Binary_log_event* Binlog_file_driver::parse_event()
  {
    Binary_log_event *parsed_event= 0;

    // This condition should never be true.
    //if (m_event_log_header.next_position > m_binlog_file_size)
    //  return parsed_event;

    switch (m_event_log_header.type_code) {
      case TABLE_MAP_EVENT:
        parsed_event= proto_table_map_event(m_binlog_file, &m_event_log_header);
        break;
      case QUERY_EVENT:
        parsed_event= proto_query_event(m_binlog_file, &m_event_log_header);
        break;
      case INCIDENT_EVENT:
        parsed_event= proto_incident_event(m_binlog_file, &m_event_log_header);
        break;
      case WRITE_ROWS_EVENT:
      case UPDATE_ROWS_EVENT:
      case DELETE_ROWS_EVENT:
        parsed_event= proto_rows_event(m_binlog_file, &m_event_log_header);
        break;
      case ROTATE_EVENT:
        {
          Rotate_event *rot= proto_rotate_event(m_binlog_file,
                                                &m_event_log_header);
          m_binlog_file_name= rot->binlog_file;
          m_binlog_offset= (unsigned long)rot->binlog_pos;
          parsed_event= rot;

          if(m_binlog_file.tellg() < m_binlog_file_size)
          // Possibly a relay log file, reset the next_position.
          {
            m_size_before_desc_event= m_binlog_file.tellg();
            m_correction_bytes= 4;
          }
          return parsed_event;
        }
        break;
      case INTVAR_EVENT:
        parsed_event= proto_intvar_event(m_binlog_file, &m_event_log_header);
        break;
     default:
        {
          // Create a dummy driver
          parsed_event= new Binary_log_event(&m_event_log_header);
        }
    }

    // Correction, ideally, this condition must always fail.
    if (m_event_log_header.next_position != m_binlog_file.tellg())
      m_binlog_file.seekg(m_event_log_header.next_position +
                          m_size_before_desc_event - m_correction_bytes, ios::beg);
    // else, missed the event boundary.

    return parsed_event;
  }
}
}

