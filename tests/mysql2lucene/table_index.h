/*
 * File:   table_index.h
 * Author: thek
 *
 * Created on den 8 september 2010, 13:47
 */

#ifndef TABLE_INDEX_H
#define	TABLE_INDEX_H
#include "binlog_event.h"
#include <map>
#include "basic_content_handler.h"

typedef std::pair<boost::uint64_t, mysql::Table_map_event *> Event_index_element;
typedef std::map<boost::uint64_t, mysql::Table_map_event *> Int2event_map;

class Table_index : public mysql::Content_handler, public Int2event_map
{
public:
 mysql::Binary_log_event *process_event(mysql::Table_map_event *tm);

 ~Table_index();

 int get_table_name(int table_id, std::string out);

};


#endif	/* TABLE_INDEX_H */
