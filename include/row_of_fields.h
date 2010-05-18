/* 
 * File:   row_of_fields.h
 * Author: thek
 *
 * Created on den 21 april 2010, 11:15
 */

#ifndef _ROW_OF_FIELDS_H
#define	_ROW_OF_FIELDS_H

#include <vector>
#include <iostream>
#include "value_adapter.h"
using namespace MySQL;
        
namespace MySQL
{

class Row_of_fields : public std::vector<Value >
{
public:
    Row_of_fields() : std::vector<Value >(0) { }
    Row_of_fields(int field_count) : std::vector<Value >(field_count) {}
    virtual ~Row_of_fields() {}

    Row_of_fields& operator=(const Row_of_fields &right);
    Row_of_fields& operator=(Row_of_fields &right);

private:

};

}

#endif	/* _ROW_OF_FIELDS_H */

