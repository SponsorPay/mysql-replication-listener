/* 
 * File:   binlog_api.h
 * Author: thek
 *
 * Created on den 23 februari 2010, 16:11
 */

#ifndef _REPEVENT_H
#define	_REPEVENT_H

#include <iosfwd>
#include <boost/iostreams/categories.hpp>
#include <boost/iostreams/positioning.hpp>
#include <boost/iostreams/concepts.hpp>
#include <boost/asio.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <list>
#include "binlog_event.h"
#include "binlog_driver.h"
#include "tcp_driver.h"
#include "basic_content_handler.h"
#include "basic_transaction_parser.h"
#include "field_iterator.h"
#include "rowset.h"
#include "access_method_factory.h"

namespace io = boost::iostreams;

namespace MySQL
{

/**
 * Returns true if the event is consumed
 */
typedef boost::function< bool (Binary_log_event *& )> Event_content_handler;

class Dummy_driver : public system::Binary_log_driver
{
  public:
  Dummy_driver() {}
  virtual ~Dummy_driver() {}

  virtual int connect() { return 1; }

  virtual int wait_for_next_event(MySQL::Binary_log_event * &event) {  }

  virtual int set_position(const std::string &str, unsigned long position) { return false; }

  virtual int get_position(std::string &str, unsigned long &position) { return false; }
};

class Content_handler;

typedef std::list<Content_handler *> Content_handler_pipeline;

class Binary_log {
private:
  system::Binary_log_driver *m_driver;
  Dummy_driver m_dummy_driver;
  Content_handler_pipeline m_content_handlers;
  unsigned long m_binlog_position;
  std::string m_binlog_file;
public:
  Binary_log(system::Binary_log_driver *drv);
  ~Binary_log() {}

  int connect();

  /**
   * Blocking attempt to get the next binlog event from the stream
   */
  int wait_for_next_event(Binary_log_event * &event);

  
  /**
   * Inserts/removes content handlers in and out of the chain
   * The Content_handler_pipeline is a derived std::list
   */
  Content_handler_pipeline *content_handler_pipeline();

  /**
   * Set the binlog position (filename, position)
   *
   * @return False on success, true if the operation failed.
   */
  int position(const std::string &filename, unsigned long position);

  /**
   * Set the binlog position using current filename
   * @param position Requested position
   *
   * @return False on success, true if the operation failed.
   */
  int position(unsigned long position);

  /**
   * Fetch the binlog position for the current file
   */
  unsigned long position(void);

  /**
   * Fetch the current active binlog file name.
   * @param[out] filename
   *
   * @return The file position
   */
  unsigned long position(std::string &filename);

};

}

#endif	/* _REPEVENT_H */

