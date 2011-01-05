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

#ifndef _BINLOG_DRIVER_H
#define	_BINLOG_DRIVER_H
#include "binlog_event.h"

namespace MySQL { namespace system {

class Binary_log_driver
{
public:
  /**
   *
   */
  Binary_log_driver() {}
  ~Binary_log_driver() {}

  /**
   * Connect to the binary log using previously declared connection parameters
   * @return Success or error code
   * @retval 0 Success
   * @retval >0 Error code (to be specified)
   */
  virtual int connect()= 0;


  /**
   * Blocking attempt to get the next binlog event from the stream
   * @param event [out] Pointer to a binary log event to be fetched.
   */
  virtual int wait_for_next_event(MySQL::Binary_log_event **event)= 0;

  /**
   * Set the reader position
   * @param str The file name
   * @param position The file position
   *
   * @return False on success and True if an error occurred.
   */
  virtual int set_position(const std::string &str, unsigned long position)= 0;

  /**
   * Get the read position.
   *
   * @param[out] string_ptr Pointer to location where the filename will be stored.
   * @param[out] position_ptr Pointer to location where the position will be stored.
   *
   * @retval 0 Success
   * @retval >0 Error code
   */
  virtual int get_position(std::string *filename_ptr, unsigned long *position_ptr) = 0;
};

}} // end namespace MySQL::system
#endif	/* _BINLOG_DRIVER_H */

