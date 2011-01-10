/*
 * File:   table_delete.h
 * Author: thek
 *
 * Created on den 17 juni 2010, 14:28
 */

#ifndef _TABLE_DELETE_H
#define	_TABLE_DELETE_H
#include <string>
#include "binlog_api.h"

void table_delete(std::string table_name, mysql::Row_of_fields &fields);

#endif	/* _TABLE_DELETE_H */
