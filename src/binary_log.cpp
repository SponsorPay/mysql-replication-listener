#include "repevent.h"

using namespace MySQL;
using namespace MySQL::system;

Binary_log::Binary_log(Binary_log_driver *drv) : m_driver(drv), m_binlog_position(4), m_binlog_file("")
{
  m_parser_func= boost::ref(m_default_parser);
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
  unsigned long old_position;
  m_driver->get_position(filename, old_position);
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

int Binary_log::connect()
{
  return m_driver->connect();
}

