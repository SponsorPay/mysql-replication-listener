/* 
 * File:   basic_transaction_parser.h
 * Author: thek
 *
 * Created on den 30 mars 2010, 19:33
 */

#ifndef _BASIC_TRANSACTION_PARSER_H
#define	_BASIC_TRANSACTION_PARSER_H

/*
  TODO The Transaction_log_event and Basic_transaction_parser will be removed
  from this library and replaced  with a table map indexer instead which can be
  used to retrive table names.
*/

#include <list>
#include <boost/cstdint.hpp>
#include "binlog_event.h"
#include "basic_content_handler.h"

#include <iostream>

namespace MySQL {
typedef std::pair<boost::uint64_t, Binary_log_event *> Event_index_element;
typedef std::map<boost::uint64_t, Binary_log_event *> Int_to_Event_map;
class Transaction_log_event : public Binary_log_event
{
public:
    Transaction_log_event() : Binary_log_event() {}
    Transaction_log_event(Log_event_header *header) : Binary_log_event(header) {}
    virtual ~Transaction_log_event();

    Int_to_Event_map &table_map() { return m_table_map; }
    /**
     * Index for easier table name look up
     */
    Int_to_Event_map m_table_map;

    std::list<Binary_log_event *> m_events;
};

Transaction_log_event *create_transaction_log_event(void);

class Basic_transaction_parser : public MySQL::Content_handler
{
public:
  Basic_transaction_parser() : MySQL::Content_handler()
  {
      m_transaction_state= NOT_IN_PROGRESS;
  }

  MySQL::Binary_log_event *process_event(MySQL::Query_event *ev);
  MySQL::Binary_log_event *process_event(MySQL::Row_event *ev);
  MySQL::Binary_log_event *process_event(MySQL::Table_map_event *ev);
  MySQL::Binary_log_event *process_event(MySQL::Xid *ev);
  MySQL::Binary_log_event *process_event(MySQL::Binary_log_event *ev) {return ev; }

private:
  boost::uint32_t m_start_time;
  enum Transaction_states { STARTING, IN_PROGRESS, COMMITTING, NOT_IN_PROGRESS } ;
  enum Transaction_states m_transaction_state;
  std::list <MySQL::Binary_log_event *> m_event_stack;
  MySQL::Binary_log_event *process_transaction_state(MySQL::Binary_log_event *ev);
};

} // end namespace

#endif	/* _BASIC_TRANSACTION_PARSER_H */

