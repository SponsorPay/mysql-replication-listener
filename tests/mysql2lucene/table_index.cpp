#include "table_index.h"

mysql::Binary_log_event *Table_index::process_event(mysql::Table_map_event *tm)
{
 if (find(tm->table_id) == end())
   insert(Event_index_element(tm->table_id,tm));

 /* Consume this event so it won't be deallocated beneith our feet */
 return 0;
}

Table_index::~Table_index ()
{
  Int2event_map::iterator it= begin();
  do
  {
    delete it->second;
  } while( ++it != end());
}

int Table_index::get_table_name(int table_id, std::string out)
{
  iterator it;
  if ((it= find(table_id)) == end())
  {
    std::stringstream os;
    os << "unknown_table_" << table_id;
    out.append(os.str());
    return 1;
  }

  out.append(it->second->table_name);
  return 0;
}
