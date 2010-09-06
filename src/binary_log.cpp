#include <list>

#include "binlog_api.h"
#include <boost/foreach.hpp>

using namespace MySQL;
using namespace MySQL::system;
namespace MySQL
{
Binary_log::Binary_log(Binary_log_driver *drv) : m_binlog_position(4), m_binlog_file("")
{
  if (drv == NULL)
  {
    m_driver= &m_dummy_driver;
  }
  else
   m_driver= drv;
}

Content_handler_pipeline *Binary_log::content_handler_pipeline(void)
{
  return &m_content_handlers;
}

void Binary_log::wait_for_next_event(MySQL::Binary_log_event * &event)
{
  bool handler_code;

  MySQL::Injection_queue reinjection_queue;

  do {
    handler_code= false;
    if (!reinjection_queue.empty())
    {
      event= reinjection_queue.front();
      reinjection_queue.pop_front();
    }
    else m_driver->wait_for_next_event(event);
    m_binlog_position= event->header()->next_position;
    MySQL::Content_handler *handler;

    BOOST_FOREACH(handler, m_content_handlers)
    {
      if (event)
      {
        handler->set_injection_queue(&reinjection_queue);
        event= handler->internal_process_event(event);
      }
    }
  } while(event == 0 || !reinjection_queue.empty());

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

}

