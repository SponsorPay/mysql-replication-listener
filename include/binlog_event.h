/* 
 * File:   binlog_event.h
 * Author: thek
 *
 * Created on den 8 mars 2010, 15:52
 */

#ifndef _BINLOG_EVENT_H
#define	_BINLOG_EVENT_H

#include <boost/cstdint.hpp>
#include <list>
#include <boost/asio.hpp>

namespace MySQL
{
/**
  @enum Log_event_type

  Enumeration type for the different types of log events.
*/
enum Log_event_type
{
  /*
    Every time you update this enum (when you add a type), you have to
    fix Format_description_log_event::Format_description_log_event().
  */
  UNKNOWN_EVENT= 0,
  START_EVENT_V3= 1,
  QUERY_EVENT= 2,
  STOP_EVENT= 3,
  ROTATE_EVENT= 4,
  INTVAR_EVENT= 5,
  LOAD_EVENT= 6,
  SLAVE_EVENT= 7,
  CREATE_FILE_EVENT= 8,
  APPEND_BLOCK_EVENT= 9,
  EXEC_LOAD_EVENT= 10,
  DELETE_FILE_EVENT= 11,
  /*
    NEW_LOAD_EVENT is like LOAD_EVENT except that it has a longer
    sql_ex, allowing multibyte TERMINATED BY etc; both types share the
    same class (Load_log_event)
  */
  NEW_LOAD_EVENT= 12,
  RAND_EVENT= 13,
  USER_VAR_EVENT= 14,
  FORMAT_DESCRIPTION_EVENT= 15,
  XID_EVENT= 16,
  BEGIN_LOAD_QUERY_EVENT= 17,
  EXECUTE_LOAD_QUERY_EVENT= 18,

  TABLE_MAP_EVENT = 19,

  /*
    These event numbers were used for 5.1.0 to 5.1.15 and are
    therefore obsolete.
   */
  PRE_GA_WRITE_ROWS_EVENT = 20,
  PRE_GA_UPDATE_ROWS_EVENT = 21,
  PRE_GA_DELETE_ROWS_EVENT = 22,

  /*
    These event numbers are used from 5.1.16 and forward
   */
  WRITE_ROWS_EVENT = 23,
  UPDATE_ROWS_EVENT = 24,
  DELETE_ROWS_EVENT = 25,

  /*
    Something out of the ordinary happened on the master
   */
  INCIDENT_EVENT= 26,

          /*
           * A user defined event
           */
          USER_DEFINED= 27,
  /*
    Add new events here - right above this comment!
    Existing events (except ENUM_END_EVENT) should never change their numbers
  */


  ENUM_END_EVENT /* end marker */
};

namespace system {
/**
 * Convenience function to get the string representation of a binlog event.
 */
const char* get_event_type_str(Log_event_type type);
} // end namespace system

#define LOG_EVENT_HEADER_SIZE 20
class Log_event_header
{
public:
  boost::uint8_t  marker; // always 0 or 0xFF
  boost::uint32_t timestamp;
  boost::uint8_t  type_code;
  boost::uint32_t server_id;
  boost::uint32_t event_length;
  boost::uint32_t next_position;
  boost::uint16_t flags;
};


class Event_body;

/**
 * TODO Base class for events. Implementation is in body()
 */
//template <class Event_body >
class Binary_log_event
{
public:
    Binary_log_event()
    {
        /*
          An event length of 0 indicates that the header isn't initialized
         */
        m_header.event_length= 0;
        m_header.type_code=    0;
        m_payload= 0;
    }

    ~Binary_log_event();

    /**
     * Helper method
     */
    enum Log_event_type get_event_type() { return (enum Log_event_type) m_header.type_code; }

    /**
     * Return a pointer to the header of the log event
     */
    Log_event_header *header() { return &m_header; }

    /**
     *
     */
    Event_body *body() { return m_payload; }




private:
    Log_event_header m_header;
    void body(Event_body *body) { m_payload= body; }
    friend class Event_body;
    Event_body *m_payload;
};

typedef Binary_log_event* Binary_log_event_ptr;

class Event_body
{
public:
    Event_body(Binary_log_event_ptr &ev) { ev->body(this); }
    virtual ~Event_body() {}
};

class Query_event: public Event_body
{
public:
    Query_event(Binary_log_event_ptr &ev) : Event_body(ev) {}
    boost::uint32_t thread_id;
    boost::uint32_t exec_time;
    boost::uint8_t  db_name_len;
    boost::uint16_t error_code;
    boost::uint16_t var_size;

    std::string db_name;
    std::string query;
};

class Rotate_event: public Event_body
{
public:
    Rotate_event(Binary_log_event_ptr &ev) : Event_body(ev) {}
    std::string binlog_file;
    boost::uint64_t binlog_pos;
};

class Format_event: public Event_body
{
public:
    Format_event(Binary_log_event_ptr &ev) : Event_body(ev) {}
    boost::uint16_t binlog_version;
    std::string master_version;
    boost::uint32_t created_ts;
    boost::uint8_t  log_header_len;
    std::string perm_events;
    boost::uint32_t  perm_events_len;
};

class User_var_event: public Event_body
{
public:
    User_var_event(Binary_log_event_ptr &ev) : Event_body(ev) {}
    boost::uint32_t name_len;
    std::string name;

    boost::uint8_t is_null;
    boost::uint8_t type;
    boost::uint32_t charset; /* charset of the string */

    boost::uint32_t value_len;
    std::string value; /* encoded in binary speak, depends on .type */
};

class Table_map_event: public Event_body
{
public:
    Table_map_event(Binary_log_event_ptr &ev) : Event_body(ev) {}
    boost::uint64_t table_id;
    boost::uint16_t flags;

    boost::uint8_t db_name_len;
    std::string db_name;
    boost::uint8_t table_name_len;
    std::string table_name;

    boost::uint64_t columns_len;
    std::string columns;

    boost::uint64_t metadata_len;
    std::string metadata;

    boost::uint32_t null_bits_len;
    std::string null_bits;

};

typedef std::vector<char*> Column_images;

class Row_event: public Event_body
{
public:
    Row_event(Binary_log_event_ptr &ev) : Event_body(ev) {}
    boost::uint64_t table_id;
    boost::uint16_t flags;
    boost::uint64_t columns_len;

    std::string used_columns;
    boost::uint32_t null_bits_len;

    boost::uint32_t row_len;
    std::string row;
};

class Int_var_event: public Event_body
{
public:
    Int_var_event(Binary_log_event_ptr &ev) : Event_body(ev) {}
    boost::uint8_t  type;
    boost::uint64_t value;
};

class Incident_event: public Event_body
{
public:
    Incident_event(Binary_log_event_ptr &ev) : Event_body(ev) {}
    boost::uint8_t  type;
    std::string message;
};

class Xid: public Event_body
{
public:
    Xid(Binary_log_event_ptr &ev) : Event_body(ev) {}
    boost::uint64_t xid_id;
};

typedef std::pair<boost::uint64_t, Binary_log_event_ptr> Event_index_element;
typedef std::map<boost::uint64_t, Binary_log_event_ptr> Int_to_Event_map;
class Transaction_log_event : public Event_body
{
public:
    Transaction_log_event(Binary_log_event_ptr &ev) : Event_body(ev) {}
    virtual ~Transaction_log_event();
    
    Int_to_Event_map &table_map() { return m_table_map; }
//private:

    /**
     * Index for easier table name look up
     */
    Int_to_Event_map m_table_map;

    std::list<Binary_log_event_ptr> m_events;
};

Binary_log_event_ptr create_transaction_log_event(void);
Binary_log_event_ptr create_incident_event(unsigned int type, const char *message, unsigned long pos= 0);

} // end namespace MySQL

#endif	/* _BINLOG_EVENT_H */

