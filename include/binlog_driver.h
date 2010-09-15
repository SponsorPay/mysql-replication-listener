/* 
 * File:   binlog_driver.h
 * Author: thek
 *
 * Created on den 8 mars 2010, 15:35
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
  virtual int wait_for_next_event(MySQL::Binary_log_event * &event)= 0;

  /**
   * Set the reader position
   * @param str The file name
   * @param position The file position
   *
   * @return False on success and True if an error occurred.
   */
  virtual int set_position(const std::string &str, unsigned long position)= 0;

  virtual int get_position(std::string &str, unsigned long &position) = 0;
};

}} // end namespace MySQL::system
#endif	/* _BINLOG_DRIVER_H */

