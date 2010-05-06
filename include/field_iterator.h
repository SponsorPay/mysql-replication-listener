/* 
 * File:   field_iterator.h
 * Author: thek
 *
 * Created on den 8 april 2010, 15:24
 */

#ifndef _FIELD_ITERATOR_H
#define	_FIELD_ITERATOR_H
#include "binlog_event.h"
#include "value_adapter.h"
#include "row_of_fields.h"
#include <vector>

using namespace MySQL;

namespace MySQL {

int lookup_metadata_field_size(enum MySQL::system::enum_field_types field_type);
boost::uint32_t extract_metadata(Table_map_event *map, int col_no);

class Row_event_iterator : public std::iterator<std::forward_iterator_tag, Row_of_fields>
{
public:
 /*
  typedef std::forward_iterator_tag iterator_category;
  typedef Row_of_fields value_type;
  typedef Row_of_fields* pointer;
  typedef Row_of_fields& reference;
  typedef size_t size_type;
  typedef ptrdiff_t difference_type;
*/
  Row_event_iterator() : m_row_event(0), m_table_map(0), m_new_field_offset_calculated(0), m_field_offset(0)
  {

  }

  Row_event_iterator(Row_event *row_event, Table_map_event *table_map)
    : m_row_event(row_event), m_table_map(table_map), m_new_field_offset_calculated(0)
  {
     // m_field_offset= row_event->null_bits_len;
      m_field_offset= 0;
  }

  Row_of_fields operator*();
  
  //const Row_of_fields& operator*() const;

  Row_event_iterator& operator++();
  
  Row_event_iterator operator++(int);

  bool operator==(const Row_event_iterator& x) const;
  
  bool operator!=(const Row_event_iterator& x) const;

  //Row_iterator end() const;
private:
    size_t fields(Row_of_fields& fields_vector );
    Row_event *m_row_event;
    Table_map_event *m_table_map;
    unsigned long m_new_field_offset_calculated;
    unsigned long m_field_offset;
};

}


#endif	/* _FIELD_ITERATOR_H */

