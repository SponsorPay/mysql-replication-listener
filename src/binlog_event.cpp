#include "binlog_event.h"
#include <iostream>
namespace MySQL
{

namespace system {

const char* get_event_type_str(Log_event_type type)
{
  switch(type) {
  case START_EVENT_V3:  return "Start_v3";
  case STOP_EVENT:   return "Stop";
  case QUERY_EVENT:  return "Query";
  case ROTATE_EVENT: return "Rotate";
  case INTVAR_EVENT: return "Intvar";
  case LOAD_EVENT:   return "Load";
  case NEW_LOAD_EVENT:   return "New_load";
  case SLAVE_EVENT:  return "Slave";
  case CREATE_FILE_EVENT: return "Create_file";
  case APPEND_BLOCK_EVENT: return "Append_block";
  case DELETE_FILE_EVENT: return "Delete_file";
  case EXEC_LOAD_EVENT: return "Exec_load";
  case RAND_EVENT: return "RAND";
  case XID_EVENT: return "Xid";
  case USER_VAR_EVENT: return "User var";
  case FORMAT_DESCRIPTION_EVENT: return "Format_desc";
  case TABLE_MAP_EVENT: return "Table_map";
  case PRE_GA_WRITE_ROWS_EVENT: return "Write_rows_event_old";
  case PRE_GA_UPDATE_ROWS_EVENT: return "Update_rows_event_old";
  case PRE_GA_DELETE_ROWS_EVENT: return "Delete_rows_event_old";
  case WRITE_ROWS_EVENT: return "Write_rows";
  case UPDATE_ROWS_EVENT: return "Update_rows";
  case DELETE_ROWS_EVENT: return "Delete_rows";
  case BEGIN_LOAD_QUERY_EVENT: return "Begin_load_query";
  case EXECUTE_LOAD_QUERY_EVENT: return "Execute_load_query";
  case INCIDENT_EVENT: return "Incident";
  case USER_DEFINED: return "User defined";
  default: return "Unknown";				/* impossible */
  }
}

} // end namespace system

/*
Binary_log_event *construct_event(Log_event_header &h)
{
  switch( h.type_code)
  {
  case UNKNOWN_EVENT:
  case START_EVENT_V3:
  case QUERY_EVENT:
  case STOP_EVENT:
  case ROTATE_EVENT:
  case INTVAR_EVENT:
  case LOAD_EVENT:
  case SLAVE_EVENT:
  case CREATE_FILE_EVENT:
  case APPEND_BLOCK_EVENT:
  case EXEC_LOAD_EVENT:
  case DELETE_FILE_EVENT:
  case NEW_LOAD_EVENT:
  case RAND_EVENT:
  case USER_VAR_EVENT:
  case FORMAT_DESCRIPTION_EVENT:
  case XID_EVENT:
  case BEGIN_LOAD_QUERY_EVENT:
  case EXECUTE_LOAD_QUERY_EVENT:
  case TABLE_MAP_EVENT:
  case PRE_GA_WRITE_ROWS_EVENT:
  case PRE_GA_UPDATE_ROWS_EVENT:
  case PRE_GA_DELETE_ROWS_EVENT:
  case WRITE_ROWS_EVENT:
  case UPDATE_ROWS_EVENT:
  case DELETE_ROWS_EVENT:
  case INCIDENT_EVENT:
    
  default:
      
  }
} */


Binary_log_event::~Binary_log_event()
{
  delete m_payload;
}

Binary_log_event *create_transaction_log_event(void)
{
    Binary_log_event *ev= new Binary_log_event();
    ev->header()->type_code= USER_DEFINED;
    Transaction_log_event *trans= new Transaction_log_event(ev);
    return ev;
};

Transaction_log_event::~Transaction_log_event()
{
  std::cout << "I'm "<< std::hex << (unsigned long) this << " a poor Transaction_log_event and I'm about to die." << std::endl;
  while (m_events.size() > 0)
  {
    Binary_log_event_ptr event= m_events.back();
    m_events.pop_back();
    delete(event);
  }

  Int_to_Event_map::iterator it;
  for(it = m_table_map.begin(); it != m_table_map.end();)
  {
    //delete it->second;
    m_table_map.erase(it++);
  }
}

} // end namespace MySQL
