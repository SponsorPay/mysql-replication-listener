#include "binlog_event.h"
#include "basic_transaction_parser.h"
#include "protocol.h"
#include "value_adapter.h"
#include <boost/any.hpp>
#include <boost/lexical_cast.hpp>
#include <iostream>
#include "field_iterator.h"

bool Basic_transaction_parser::operator()(MySQL::Binary_log_event_ptr &ev)
{
  switch(ev->get_event_type())
  {
    case MySQL::QUERY_EVENT:
    {
      MySQL::Query_event *qev= static_cast<MySQL::Query_event *>(ev->body());
      if (qev->query == "BEGIN")
      {
        //std::cout << "Transaction has started!" << std::endl;
        m_transaction_state= STARTING;
      }
      if (qev->query == "COMMIT")
      {
        //std::cout << "Transaction is committed!" << std::endl;
        m_transaction_state= COMMITTING;
      }
    }
    break;
    case MySQL::XID_EVENT:
    {
      /* Transaction capable engines end their transactions with XID */
      //std::cout << "Transaction is committed!" << std::endl;
      m_transaction_state= COMMITTING;
    }
    break;
  }

  switch(m_transaction_state)
  {
      case IN_PROGRESS:
      {
        switch (ev->get_event_type())
        {
          case MySQL::WRITE_ROWS_EVENT:
          case MySQL::DELETE_ROWS_EVENT:
          case MySQL::UPDATE_ROWS_EVENT:
          case MySQL::TABLE_MAP_EVENT:
            m_event_stack.push_back(ev);
            return true;
          default:
            return false;
        }
      }
      case STARTING:
      {
        m_transaction_state= IN_PROGRESS;
        m_start_time= ev->header()->timestamp;
        delete ev; // drop the begin event
        return true;
      }
      case COMMITTING:
      {
        delete ev; // drop the commit event
        ev= MySQL::create_transaction_log_event();
        /**
         * Propagate the start time for the transaction to the newly created
         * event.
         */
        ev->header()->timestamp= m_start_time;
        MySQL::Transaction_log_event *trans= static_cast<MySQL::Transaction_log_event*>(ev->body());
          
        //std::cout << "There are " << m_event_stack.size() << " events in the transaction: ";
        while( m_event_stack.size() > 0)
        {
          MySQL::Binary_log_event_ptr event= m_event_stack.front();
          m_event_stack.pop_front();
          switch(event->get_event_type())
          {
            case MySQL::TABLE_MAP_EVENT:
            {
              /*
               Index the table name with a table id to ease lookup later.
              */
              MySQL::Table_map_event *tm= static_cast<MySQL::Table_map_event *>(event->body());
              trans->m_table_map.insert(MySQL::Event_index_element(tm->table_id,event));
              trans->m_events.push_back(event);
            }
            break;
            case MySQL::WRITE_ROWS_EVENT:
            case MySQL::DELETE_ROWS_EVENT:
            case MySQL::UPDATE_ROWS_EVENT:
            {
              trans->m_events.push_back(event);

              /*
               * Propagate last known next position
               */
              ev->header()->next_position= event->header()->next_position;
            }
            break;
            default:
              delete event;

          }
        } // end while
        m_transaction_state= NOT_IN_PROGRESS;
        return false;
      }
      case NOT_IN_PROGRESS:
      default:
        return false;
  } // end switch
  
}




