/* 
 * File:   repevent.h
 * Author: thek
 *
 * Created on den 23 februari 2010, 16:11
 */

#ifndef _REPEVENT_H
#define	_REPEVENT_H

#include <iosfwd>
#include <boost/iostreams/categories.hpp>
#include <boost/iostreams/positioning.hpp>
#include <boost/iostreams/concepts.hpp>  // seekable_device
#include <boost/asio.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <list>
#include "binlog_event.h"
#include "binlog_driver.h"


namespace io = boost::iostreams;

namespace MySQL
{

/**
 * Returns true if the event is consumed
 */
typedef boost::function< bool (Binary_log_event_ptr& )> Event_parser_func;

class Default_event_parser
{
public:
    Default_event_parser() {}
    ~Default_event_parser() { /* TODO: make sure the stack is empty */ }

    /**
     * The parser takes on token from a stream of 'events' and transform it
     * into a new event which is delivered to the client application
     * as a result of wait_for_next_event(). An event might set a state
     * in the parser and be entirely consumed (deleted).
     *
     * @param ev A pointer to an event
     *
     */
     bool operator()(Binary_log_event_ptr &ev)
     {
        return false;
     }
    
private:
};

template <class DeviceDriver >
class Binary_log {
private:
  DeviceDriver *m_driver;
  Default_event_parser m_default_parser;
  Event_parser_func m_parser_func;
  unsigned long m_binlog_position;
  std::string m_binlog_file;
public:
  Binary_log();
  ~Binary_log() {}

  int open(const char *url);

  /**
   * Attach a device driver for reading and writing to the binlog event
   * stream.
   */
  int register_device_driver(DeviceDriver *driver)
  {
      delete m_driver;
      m_driver= driver;
      // TODO init driver?
      return 1;
  }

  /**
   * Blocking attempt to get the next binlog event from the stream
   */
  int wait_for_next_event(Binary_log_event_ptr &event);


  /**
   * Non-blocking fetch from event stream
   */
  int poll_next_event(Binary_log_event_ptr &event) { m_driver->poll_next_event(event); return 1; }


  /**
   * Pluggable parser for accumulating events
   */
  void parser(Event_parser_func &f) { m_parser_func= boost::ref(f); }


  /**
   * Set the binlog position (filename, position)
   *
   * @return False on success, true if the operation failed.
   */
  bool position(const std::string &filename, unsigned long position);

  /**
   * Set the binlog position using current filename
   * @param position Requested position
   *
   * @return False on success, true if the operation failed.
   */
  bool position(unsigned long position);

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

