/* 
 * File:   rowset.h
 * Author: thek
 *
 * Created on den 18 april 2010, 19:24
 */

#ifndef _ROWSET_H
#define	_ROWSET_H

#include "field_iterator.h"
#include "resultset_iterator.h"
#include <boost/function.hpp>
#include <boost/iterator.hpp>

using namespace MySQL;

namespace MySQL {

class Row_event;
class Table_map_event;

class Row_event_set
{
public:
    typedef Row_event_iterator<Row_of_fields > iterator;
    typedef Row_event_iterator<Row_of_fields const > const_iterator;

    Row_event_set(Row_event *arg1, Table_map_event *arg2) { source(arg1, arg2); }

    iterator begin() { return iterator(m_row_event, m_table_map_event); }
    iterator end() { return iterator(); }
    const_iterator begin() const { return const_iterator(m_row_event, m_table_map_event); }
    const_iterator end() const { return const_iterator(); }

private:
    void source(Row_event *arg1, Table_map_event *arg2) { m_row_event= arg1; m_table_map_event= arg2; }
    Row_event *m_row_event;
    Table_map_event *m_table_map_event;
};

}
#endif	/* _ROWSET_H */

