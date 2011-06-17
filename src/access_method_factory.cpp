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
#include "access_method_factory.h"
#include "tcp_driver.h"
#include "file_driver.h"

namespace mysql {
namespace system {
Binary_log_driver *create_transport(const char *url)
{
  // TODO Parse the URL (using boost::spirit) and call the proper device driver
  //      factory.
  // temporary code for test purpose follows below
  char *url_copy= strdup(url);
  const char *mysql_access_method= "mysql://";
  const char *file_access_method= "file://";

  if (strstr(url_copy, mysql_access_method))
    return (parse_mysql_url(url_copy, mysql_access_method));
  else if (strstr(url_copy, file_access_method))
    return (parse_file_url(url_copy, file_access_method));
  else
    goto err;                                   // Not supported!

err:
  free(url_copy);
  return 0;
}


Binary_log_driver *parse_mysql_url(char *url, const char
                                   *mysql_access_method)
{
  char user[21];
  char host[50];
  char port[6];
  char pass[21];
  user[0]= 0;
  host[0]= 0;
  port[0]= 0;
  pass[0]= 0;
  unsigned user_length= 0;
  unsigned host_length= 0;
  unsigned port_length= 0;
  unsigned pass_length= 0;
  unsigned long portno= 0;
  char *front, *scan;
  front= strstr(url, mysql_access_method);
  if (front == 0)
      goto err;
  front += strlen(mysql_access_method);
   scan= strpbrk(front,":@");
  if (scan == 0)
      goto err;
  user_length= scan - front;
  if (user_length > 20)
      goto err;
  memcpy(user, front, user_length);
  user[user_length]= '\0';
  front = scan;
  if (*front == ':')
  {
    front += 1;
    scan= strpbrk(front, "@");
    if (scan == 0)
      goto err;
    pass_length= scan - front;
    memcpy(pass, front, pass_length);
    pass[pass_length] = '\0';
    front = scan;
  }

  if (*front != '@')
    goto err;
  front += 1;
  scan= strpbrk(front,":");
  if (scan != 0)
    host_length= scan - front;
  else
    host_length= strlen(front);
  if (host_length > 49)
    goto err;
  memcpy(host,front, host_length);
  host[host_length]= '\0';
  front= scan;
  if (front && *front == ':')
  {
    front +=1;
    port_length= strlen(front);
    if (port_length > 5)
      goto err;
    memcpy(port, front, port_length);
    port[port_length]= '\0';
    portno= atol(port);
  }

  free(url);
  return new system::Binlog_tcp_driver(user, pass, host, portno);
err:
  free(url);
  return 0;
}


Binary_log_driver *parse_file_url(char *url, const char
                                  *file_access_method)
{
  char filename[120];
  unsigned scheme_length= strlen(file_access_method);
  unsigned filename_length= strlen(url) - scheme_length;

  memcpy(filename, url + scheme_length, filename_length);
  filename[filename_length]= '\0';

  free(url);
  return new system::Binlog_file_driver(filename);
}


} // end namespace system
} // end namespace mysql
