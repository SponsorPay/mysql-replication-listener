/*
 * File:   table_update.h
 * Author: thek
 *
 * Created on den 17 juni 2010, 14:27
 */

#ifndef _TABLE_UPDATE_H
#define	_TABLE_UPDATE_H
#include <string>
#include "binlog_api.h"
void table_update(std::string table_name, MySQL::Row_of_fields &old_fields, MySQL::Row_of_fields &new_fields);

#endif	/* _TABLE_UPDATE_H */
