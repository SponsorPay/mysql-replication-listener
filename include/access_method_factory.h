/* 
 * File:   Access_method_factory.h
 * Author: thek
 *
 * Created on den 11 maj 2010, 15:02
 */

#ifndef _ACCESS_METHOD_FACTORY_H
#define	_ACCESS_METHOD_FACTORY_H

#include "binlog_driver.h"

namespace MySQL { 
system::Binary_log_driver *create_transport(const char *url);


}


#endif	/* _ACCESS_METHOD_FACTORY_H */

