/* 
 * File:   maintest.cpp
 * Author: thek
 *
 * Created on den 23 februari 2010, 16:10
 */

#include <stdlib.h>
#include <boost/foreach.hpp>
#include "repevent.h"

void table_insert(std::string &table_name,MySQL::Row_of_fields &fields);
void table_update(std::string &table_name,MySQL::Row_of_fields &old_fields, MySQL::Row_of_fields &new_fields);
void table_delete(std::string &table_name,MySQL::Row_of_fields &fields);
/*
 * 
 */
int main(int argc, char** argv)
{

  MySQL::Binary_log binlog(MySQL::create_transport("mysql://pear:apple@127.0.0.1:13000"));
  while (binlog.connect())
  {
    std::cout << "Can't connect to the master. Retrying... " <<std::endl;
    sleep(2);
  };

  
  /*
   Attach a custom event parser which produces user defined events
  */
  Basic_transaction_parser transaction_parser;
  MySQL::Event_parser_func f= boost::ref(transaction_parser);
  binlog.parser(f);

  MySQL::Binary_log_event_ptr event;
  //binlog.position("searchbin.000001",4);
  
  while(true)
  {
    /*
     Pull events from the master. This is the heart beat of the event listener.
    */
    binlog.wait_for_next_event(event);

    /*
     Print the event
    */
    time_t localtime= event->header()->timestamp;
    char *localtime_str= ctime(&localtime);
    localtime_str[strlen(localtime_str)-1]= 0;
    std::cout << "["<< localtime_str << "] Event type: "
              << MySQL::system::get_event_type_str(event->get_event_type())
              << " length: " << event->header()->event_length
              << " next pos: " << event->header()->next_position
              << std::endl;

    /*
     Perform a special action based on event type
    */

    switch(event->header()->type_code)
    {
    case MySQL::QUERY_EVENT:
      {
        const MySQL::Query_event *qev= static_cast<const MySQL::Query_event *>(event->body());
        std::cout << "Query event: query=" << qev->query << " db= " << qev->db_name <<  std::endl;
        
        
      }
      break;
    case MySQL::ROTATE_EVENT:
      {
        MySQL::Rotate_event *rot= static_cast<MySQL::Rotate_event *>(event->body());
        std::cout << "filename: "
                  << rot->binlog_file.c_str()
                  << " pos: "
                  << rot->binlog_pos
                  << std::endl;
      }
      break;
    case MySQL::TABLE_MAP_EVENT:
      {
        MySQL::Table_map_event *tmev= static_cast<MySQL::Table_map_event *>(event->body());
        std::cout << "Table_map_event: table_id= "<< tmev->table_id << " table_name= " << tmev->table_name << std::endl;
      }
      break;
    case MySQL::WRITE_ROWS_EVENT:
    case MySQL::DELETE_ROWS_EVENT:
    case MySQL::UPDATE_ROWS_EVENT:
      {
        MySQL::Row_event *rev= static_cast<MySQL::Row_event *>(event->body());
        std::cout << "Table id: "<< rev->table_id << " Number of columns in table: "<< rev->columns_len << " Row length: " << rev->row.size() << std::endl;
        
      }
      break;
    case MySQL::USER_DEFINED:
      {
        std::cout << "A user defined change set has arrived!" << std::endl;
        MySQL::Transaction_log_event *trans= static_cast<MySQL::Transaction_log_event *>(event->body());
        int row_count=0;
        /*
         The transaction event we created has aggregated all row events in an
         ordered list.
        */
        BOOST_FOREACH(MySQL::Binary_log_event_ptr event, trans->m_events)
        {
           switch (event->get_event_type())
           {
           case MySQL::WRITE_ROWS_EVENT:
           case MySQL::DELETE_ROWS_EVENT:
           case MySQL::UPDATE_ROWS_EVENT:

             MySQL::Row_event *rev= static_cast<MySQL::Row_event *>(event->body());
             // Add list to the transaction ... can we keep the original event?
             boost::uint64_t table_id= rev->table_id;
             // BUG: will create a new event header if the table id doesn't exist.
             Binary_log_event_ptr tmevent= trans->table_map()[table_id];
             MySQL::Table_map_event *tm= static_cast<MySQL::Table_map_event *>(tmevent->body());
             if (tm != NULL)
             {
               /*
                Each row event contains multiple rows and fields. The Row_iterator
                allows us to iterate one row at a time.
               */
               MySQL::Row_set<Row_event_feeder > rows(rev, tm);
               
               MySQL::Row_set<Row_event_feeder >::iterator it= rows.begin();
               do {
                 MySQL::Row_of_fields fields= *it;
                 if (event->get_event_type() == MySQL::WRITE_ROWS_EVENT)
                        table_insert(tm->table_name,fields);
                 if (event->get_event_type() == MySQL::UPDATE_ROWS_EVENT)
                 {
                        ++it;
                        MySQL::Row_of_fields fields2= *it;
                        table_update(tm->table_name,fields,fields2);
                 }
                 if (event->get_event_type() == MySQL::DELETE_ROWS_EVENT)
                        table_delete(tm->table_name,fields);

                 //if (tm->table_name == "links")
                 //{
                   MySQL::Row_of_fields::iterator field_it= fields.begin();
                   ++row_count;
                   do {
                      /*
                       Each row contains a vector of Value objects. The converter
                       allows us to transform the value into another
                       representation.
                      */
                      MySQL::Converter converter(*field_it);
                      std::string str;
                      converter.to_string(str);

                      std::cout << tm->table_name << " row= " << (int)row_count << " field type: " << (*field_it).type() << " Field size is: " << (*field_it).size() << " Field data is: " << str << std::endl;
                    } while(++field_it != fields.end());
                  //}
                } while (++it != rows.end());

              } // end if
              else
              {
                // TODO Handle error no such table id
              }
             } // end switch

        } // end BOOST_FOREACH
      }
      break;
    }
    std::cout << "Deleting event= " << MySQL::system::get_event_type_str(event->get_event_type()) << std::endl;
    delete event;
  }
  return (EXIT_SUCCESS);
}



void table_insert(std::string &table_name, MySQL::Row_of_fields &fields)
{
  
}


void table_update(std::string &table_name, MySQL::Row_of_fields &old_fields, MySQL::Row_of_fields &new_fields)
{

}


void table_delete(std::string &table_name, MySQL::Row_of_fields &fields)
{

}
