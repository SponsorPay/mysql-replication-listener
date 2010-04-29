/* 
 * File:   basic_transaction_parser.h
 * Author: thek
 *
 * Created on den 30 mars 2010, 19:33
 */

#ifndef _BASIC_TRANSACTION_PARSER_H
#define	_BASIC_TRANSACTION_PARSER_H

#include <list>
#include <boost/cstdint.hpp>
#include "binlog_event.h"

class Basic_transaction_parser
{
public:
  Basic_transaction_parser() {}
  enum Transaction_states { STARTING, IN_PROGRESS, COMMITTING, NOT_IN_PROGRESS } ;
  enum Transaction_states m_transaction_state;
  std::list <MySQL::Binary_log_event_ptr> m_event_stack;

  /**
   * @return TRUE if the event is consumed, FALSE if it should pass through
   */
  bool operator()(MySQL::Binary_log_event_ptr &ev);

private:
    boost::uint32_t m_start_time;
};


#endif	/* _BASIC_TRANSACTION_PARSER_H */

