/*
 * File:   table_insert.h
 * Author: thek
 *
 * Created on den 15 juni 2010, 09:34
 */

#ifndef _TABLE_INSERT_H
#define	_TABLE_INSERT_H

#include <string>
#include "binlog_api.h"

void table_insert(std::string table_name, mysql::Row_of_fields &fields);

#endif	/* _TABLE_INSERT_H */
