/* 
 * File:   resultset_iterator.h
 * Author: thek
 *
 * Created on den 21 april 2010, 11:09
 */

#ifndef _RESULTSET_ITERATOR_H
#define	_RESULTSET_ITERATOR_H

#include <iostream>
#include <boost/iterator.hpp>
#include <boost/asio.hpp>
#include "value_adapter.h"
#include "row_of_fields.h"

using namespace MySQL;

namespace MySQL
{

struct Field_packet
{
    std::string catalog;  // Length Coded String
    std::string db;       // Length Coded String
    std::string table;    // Length Coded String
    std::string org_table;// Length Coded String
    std::string name;     // Length Coded String
    std::string org_name; // Length Coded String
    boost::uint8_t marker;       // filler
    boost::uint16_t charsetnr;   // charsetnr
    boost::uint32_t length;      // length
    boost::uint8_t type;         // field type
    boost::uint16_t flags;
    boost::uint8_t decimals;
    boost::uint16_t filler;      // filler, always 0x00
    //boost::uint64_t default_value;  // Length coded binary; only in table descr.
};

typedef std::list<std::string > String_storage;

namespace system {
    void digest_result_header(std::istream &is, boost::uint64_t &field_count, boost::uint64_t extra);
    void digest_field_packet(std::istream &is, Field_packet &field_packet);
    void digest_marker(std::istream &is);
    void digest_row_content(std::istream &is, int field_count, Row_of_fields &row, String_storage &storage, bool &is_eof);
}

class Result_set_iterator;
#ifndef NoArg
typedef int NoArg;
#endif
class Result_set_feeder
{
public:
    typedef Result_set_iterator Iterator;
    typedef tcp::socket * Arg1;
    typedef NoArg Arg2;
    typedef NoArg Arg3;
    typedef NoArg Arg4;
    typedef const Iterator const_iterator;

    void source(Arg1 socket, Arg2 a, Arg3 b, Arg4 c) { m_socket= socket; digest_row_set(); }
    Iterator begin_iterator();
    Iterator end_iterator();

private:
    void digest_row_set();
    friend class Result_set_iterator;

    std::vector<Field_packet > m_field_types;
    int m_row_count;
    std::vector<Row_of_fields > m_rows;
    String_storage m_storage;
    Arg1 m_socket;
    typedef enum { RESULT_HEADER, FIELD_PACKETS, MARKER, ROW_CONTENTS, EOF_PACKET } state_t;
    state_t m_current_state;

    /**
     * The number of fields in the field packets block
     */
    boost::uint64_t m_field_count;
    /**
     * Used for SHOW COLUMNS to return the number of rows in the table
     */
    boost::uint64_t m_extra;
};


class Result_set_iterator : public boost::iterator_facade<Result_set_iterator, Row_of_fields, boost::forward_traversal_tag >
{
public:
    Result_set_iterator() : m_feeder(0), m_current_row(-1)
    {}

    explicit Result_set_iterator(Result_set_feeder *feeder) : m_feeder(feeder), m_current_row(-1)
    {
      increment();
    }

 private:
    friend class boost::iterator_core_access;

    void increment();
    

    bool equal(const Result_set_iterator& other) const
    {
        if (other.m_feeder == 0 && m_feeder == 0)
            return true;
        if (other.m_feeder == 0)
        {
            if (m_current_row == -1)
                return true;
            else
                return false;
        }
        if (m_feeder == 0)
        {
            if (other.m_current_row == -1)
                return true;
            else
                return false;
        }

        if( other.m_feeder->m_field_count != m_feeder->m_field_count)
            return false;

        Row_of_fields *row1= &m_feeder->m_rows[m_current_row];
        Row_of_fields *row2= &other.m_feeder->m_rows[m_current_row];
        for (int i=0; i< m_feeder->m_field_count; ++i)
        {
            Value val1= row1->at(i);
            Value val2= row2->at(i);
            if (val1 != val2)
                return false;
        }
        return true;
    }

    Row_of_fields &dereference() const
    {
        return m_feeder->m_rows[m_current_row];
    }

private:
    Result_set_feeder *m_feeder;
    int m_current_row;
 
};

} // end namespace MySQL



#endif	/* _RESULTSET_ITERATOR_H */

