/*
Copyright (c) 2003, 2011, Oracle and/or its affiliates. All rights
reserved.

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; version 2 of
the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
02110-1301  USA 
*/
#include "basic_content_handler.h"
#include <boost/bind.hpp>

namespace MySQL {

Content_handler::Content_handler () {}
Content_handler::~Content_handler () {}
MySQL::Binary_log_event *Content_handler::process_event(MySQL::Query_event *ev) { return ev; }
MySQL::Binary_log_event *Content_handler::process_event(MySQL::Row_event *ev) { return ev; }
MySQL::Binary_log_event *Content_handler::process_event(MySQL::Table_map_event *ev) { return ev; }
MySQL::Binary_log_event *Content_handler::process_event(MySQL::Xid *ev) { return ev; }
MySQL::Binary_log_event *Content_handler::process_event(MySQL::User_var_event *ev) { return ev; }
MySQL::Binary_log_event *Content_handler::process_event(MySQL::Incident_event *ev) { return ev; }
MySQL::Binary_log_event *Content_handler::process_event(MySQL::Rotate_event *ev) { return ev; }
MySQL::Binary_log_event *Content_handler::process_event(MySQL::Int_var_event *ev) { return ev; }
MySQL::Binary_log_event *Content_handler::process_event(MySQL::Binary_log_event *ev) { return ev; }

Injection_queue *Content_handler::get_injection_queue(void)
{
  return m_reinject_queue;
}

void Content_handler::set_injection_queue(Injection_queue *queue)
{
  m_reinject_queue= queue;
}

MySQL::Binary_log_event *Content_handler::internal_process_event(MySQL::Binary_log_event *ev)
{
 MySQL::Binary_log_event *processed_event= 0;
 switch(ev->header ()->type_code) {
 case MySQL::QUERY_EVENT:
   processed_event= process_event(static_cast<MySQL::Query_event*>(ev));
   break;
 case MySQL::WRITE_ROWS_EVENT:
 case MySQL::UPDATE_ROWS_EVENT:
 case MySQL::DELETE_ROWS_EVENT:
   processed_event= process_event(static_cast<MySQL::Row_event*>(ev));
   break;
 case MySQL::USER_VAR_EVENT:
   processed_event= process_event(static_cast<MySQL::User_var_event *>(ev));
   break;
 case MySQL::ROTATE_EVENT:
   processed_event= process_event(static_cast<MySQL::Rotate_event *>(ev));
   break;
 case MySQL::INCIDENT_EVENT:
   processed_event= process_event(static_cast<MySQL::Incident_event *>(ev));
   break;
 case MySQL::XID_EVENT:
   processed_event= process_event(static_cast<MySQL::Xid *>(ev));
   break;
 case MySQL::TABLE_MAP_EVENT:
   processed_event= process_event(static_cast<MySQL::Table_map_event *>(ev));
   break;
 /* TODO ********************************************************************/
 case MySQL::FORMAT_DESCRIPTION_EVENT:
   processed_event= process_event(ev);
   break;
 case MySQL::BEGIN_LOAD_QUERY_EVENT:
   processed_event= process_event(ev);
   break;
 case MySQL::EXECUTE_LOAD_QUERY_EVENT:
   processed_event= process_event(ev);
   break;
 case MySQL::INTVAR_EVENT:
   processed_event= process_event(ev);
   break;
 case MySQL::STOP_EVENT:
   processed_event= process_event(ev);
   break;
 case MySQL::RAND_EVENT:
   processed_event= process_event(ev);
   break;
 /****************************************************************************/
 default:
   processed_event= process_event(ev);
   break;
 }
 return processed_event;
}

} // end namespace
