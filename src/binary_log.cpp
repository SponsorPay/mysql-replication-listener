#include "repevent.h"
#include "tcp_driver.h"

using namespace MySQL;
using namespace MySQL::system;

template< class DeviceDriver >
Binary_log< DeviceDriver >::Binary_log() : m_binlog_position(4), m_binlog_file("")
{
  m_driver= new DeviceDriver();
  m_parser_func= boost::ref(m_default_parser);
}


template< class DeviceDriver >
int Binary_log<DeviceDriver >::open(const char *url)
{
  // TODO Parse the URL (using boost::spirit) and call the proper device driver
  //      factory.
  // temporary code for test purpose follows below
    char *url_copy= strdup(url);
    const char *access_method= "mysql://";
    char user[21];
    char host[50];
    char port[6];
    char pass[21];
    unsigned user_length;
    unsigned host_length;
    unsigned port_length;
    unsigned pass_length;
    unsigned long portno;
    char *front, *scan;
    front= strstr(url_copy,access_method);
    if (front == 0)
        goto err;
    front += strlen(access_method);

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
        host_length= strlen(front+1);

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

    // Only tcp driver supported so far
    free(url_copy);
    return m_driver->connect(user,pass,host,portno);

err:
    free(url_copy);
    return 1;
}


template< class DeviceDriver >
int Binary_log<DeviceDriver >::wait_for_next_event(MySQL::Binary_log_event_ptr &event)
{
  do
  {
    m_driver->wait_for_next_event(event);
    m_binlog_position= event->header()->next_position;
  } while (m_parser_func(event));
  return 1;
}


template< class DeviceDriver >
bool Binary_log<DeviceDriver >::position(const std::string &filename, unsigned long position)
{
  bool status= m_driver->set_position(filename, position);
  if (!status)
  {
    m_binlog_file= filename;
    m_binlog_position= position;
  }
  return status;
}

template< class DeviceDriver >
bool Binary_log<DeviceDriver >::position(unsigned long position)
{
  std::string filename;
  m_driver->get_position(filename, position);
  return this->position(filename, position);
}


template< class DeviceDriver >
unsigned long Binary_log<DeviceDriver >::position(void)
{
  return m_binlog_position;
}

template< class DeviceDriver >
unsigned long Binary_log<DeviceDriver >::position(std::string &filename)
{
  m_driver->get_position(m_binlog_file, m_binlog_position);
  filename= m_binlog_file;
  return m_binlog_position;
}

template class Binary_log<MySQL::system::Binlog_tcp_driver >;