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

#ifndef Noarg
typedef int NoArg;
#endif

typedef boost::function< bool (Row_of_fields *, Row_of_fields )> Join_condition;


class Row_event_feeder
{
public:
    typedef Row_event_iterator Iterator;
    typedef Row_event* Arg1;
    typedef Table_map_event* Arg2;
    typedef NoArg Arg3;
    typedef NoArg Arg4;

  //  Row_event_feeder(Row_event *arg1, Table_map_event *arg2) { source(arg1, arg2); }
    void source(Row_event *arg1, Table_map_event *arg2, Arg3 b, Arg4 c) { m_arg1= arg1; m_arg2= arg2; }
    Iterator begin_iterator() { return Row_event_iterator(m_arg1, m_arg2); }
    Iterator end_iterator() { return Row_event_iterator(); }

private:
    Row_event *m_arg1;
    Table_map_event *m_arg2;
};


template< class Left_rs, class Right_rs >
class Row_set_iterator;

template <class T >
class Row_set;

template <class Left_rs, class Right_rs >
class Join_set_feeder
{
public:
    typedef Row_set_iterator<Left_rs, Right_rs> Iterator;
    typedef Row_set<Left_rs  >* Arg1;
    typedef Row_set<Right_rs >* Arg2;
    typedef Join_condition Arg3;
    typedef NoArg Arg4;
    typedef const Iterator const_iterator;

    void source(Arg1 a, Arg2 b, Arg3 c, Arg4 d) { }
private:
    friend class Row_set_iterator<Left_rs, Right_rs>;
};


template <class T >
class Row_set 
{
public:
    //typedef MySQL::SourceFeeder<Iterator, Arg1, Arg2, Arg3, Arg4 >::iterator iterator;
    //typedef SourceFeeder::const_iterator const_iterator;
    //typedef Row_iterator iterator;
    
    typedef typename T::Arg1 arg1_t;
    typedef typename T::Arg2 arg2_t;
    typedef typename T::Arg3 arg3_t;
    typedef typename T::Arg4 arg4_t;
    typedef typename T::Iterator iterator;
    typedef typename T::Iterator const_iterator;

    Row_set(arg1_t arg1) { m_feeder.source(arg1,0,0,0); };
    Row_set(arg1_t arg1,arg2_t arg2) { m_feeder.source(arg1,arg2,0,0); };
    Row_set(arg1_t arg1,arg2_t arg2, arg3_t arg3) { m_feeder.source(arg1,arg2,arg3,0); };
    Row_set(arg1_t arg1,arg2_t arg2,arg3_t arg3,arg4_t arg4) { m_feeder.source(arg1,arg2,arg3,arg4); };
    ~Row_set() {};

    //Row_set(Row_event *re, Table_map_event *tm) { m_row_event= re; m_table_map_event= tm; }

    //Row_set(Arg1 arg1, Arg2 arg2) { m_arg1= arg1; m_arg2= arg2; }

    void soure(arg1_t row_set_ptr1, arg2_t row_set_ptr2, arg3_t join_condition, arg4_t noarg4)
    {
        // Join two row sets
    }

    /**
     * Inner join with rs on cond.
     */
    //void join(Row_set &rs, Join_condition &cond);

    /**
     * Left outer join with rs on cond.
     */
    //void outer_join(Row_set &rs, Join_condition &cond);


    //void build_index(int column_no)
    
    /**
     * Reset the row set so that the current source needs to be reconsumed.
     */
    void clear() {} ;

    /**
     * Returns a Row_set_iterator which produces a Row_of_fields on each
     * iteration.
     */
    iterator begin() { return m_feeder.begin_iterator(); }

    /**
     * Returns the end position for the row iterator
     */
    iterator end() { return m_feeder.end_iterator(); };

private:
    T m_feeder;
};


template< class Left_rs, class Right_rs >
class Row_set_iterator : public boost::iterator_facade<Row_set_iterator<Left_rs, Right_rs>, Row_of_fields const, boost::forward_traversal_tag >
{
public:
    typedef typename Row_set<Left_rs >::iterator Left_rs_iterator;
    
    explicit Row_set_iterator(Left_rs left, Right_rs right, Join_condition join)
    {
        m_left_it= m_left.begin();
        m_right_it= m_right.begin();
        m_join_predicate= join;
    }

    Row_set_iterator()
    {
        m_left_it= m_left.end();
        m_right_it= m_right.end();
    }

 private:
    friend class boost::iterator_core_access;

    void increment()
    {
        do {
            if (++m_right_it == m_right.end())
            {
                if (++m_left_it == m_left.end())
                {
                    return;
                }
                m_right_it= m_right.begin();
            }
        } while (!m_join_predicate(*m_left_it, *m_right_it));
    }

    bool equal(Row_set_iterator const& other) const
    {
        return *m_left_it == *m_right_it;
    }

    Row_of_fields const& dereference()
    {
        /*
        Row_of_fields new_field;
        BOOST_FOREACH(Value value, *m_left_it)
          new_field.push_back(value);
        BOOST_FOREACH(Value value, *m_right_it)
          new_field.push_back(value);
        return new_field;
         */
        return m_tmp_row;
    }

    Row_of_fields m_tmp_row;
    Left_rs* m_left;
    Right_rs* m_right;
     Left_rs_iterator m_left_it;
    typename Row_set<Right_rs >::iterator m_right_it;
    Join_condition m_join_predicate;

};


}
#endif	/* _ROWSET_H */

