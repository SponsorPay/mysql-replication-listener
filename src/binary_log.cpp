#include "repevent.h"
#include "tcp_driver.h"

using namespace MySQL;

Binary_log::Binary_log() : m_binlog_position(4), m_binlog_file("")
{
  m_parser_func=  boost::ref(m_default_parser);
}
int Binary_log::open(const char *url)
{
  // TODO Parse the URL (using boost::spirit) and call the proper device driver
  //      factory.
  // temporary code for test purpose follows below
  system::Binlog_tcp_driver *driver= new system::Binlog_tcp_driver();
  m_driver= driver;
  long default_port= 3434;
  std::string host(url);
  driver->connect("root","",host,default_port);
  driver->set_position("", 4);
  return 1;
}

int Binary_log::wait_for_next_event(MySQL::Binary_log_event_ptr &event)
{
  do
  {
    m_driver->wait_for_next_event(event);
    m_binlog_position= event->header()->next_position;
  } while (m_parser_func(event));
  return 1;
}


bool Binary_log::position(const std::string &filename, unsigned long position)
{
  bool status= m_driver->set_position(filename, position);
  if (!status)
  {
    m_binlog_file= filename;
    m_binlog_position= position;
  }
  return status;
}

bool Binary_log::position(unsigned long position)
{
  std::string filename;
  m_driver->get_position(filename, position);
  return this->position(filename, position);
}


unsigned long Binary_log::position(void)
{
  return m_binlog_position;
}

unsigned long Binary_log::position(std::string &filename)
{
  m_driver->get_position(m_binlog_file, m_binlog_position);
  filename= m_binlog_file;
  return m_binlog_position;
}
