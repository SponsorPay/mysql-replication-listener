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
#include "tcp_driver.h"
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
typedef boost::function< bool (Binary_log_event_ptr& )> Event_parser_func;
class Dummy_driver : public system::Binary_log_driver
{
  public:
  Dummy_driver() {}
  virtual ~Dummy_driver() {}

  virtual int connect() { return 1; }

  virtual void wait_for_next_event(MySQL::Binary_log_event_ptr &event) {  }

  virtual bool set_position(const std::string &str, unsigned long position) { return false; }

  virtual bool get_position(std::string &str, unsigned long &position) { return false; }
};

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

class Binary_log {
private:
  system::Binary_log_driver *m_driver;
  Default_event_parser m_default_parser;
  Dummy_driver m_dummy_driver;
  Event_parser_func m_parser_func;
  unsigned long m_binlog_position;
  std::string m_binlog_file;
public:
  Binary_log(system::Binary_log_driver *drv);
  ~Binary_log() {}

  int connect();

  /**
   * Blocking attempt to get the next binlog event from the stream
   */
  void wait_for_next_event(Binary_log_event_ptr &event);

  
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

