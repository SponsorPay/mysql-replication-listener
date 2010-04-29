#include "binlog_event.h"
#include "basic_transaction_parser.h"
#include "protocol.h"
#include "value_adapter.h"
#include <boost/any.hpp>
#include <boost/lexical_cast.hpp>
#include <iostream>
#include "field_iterator.h"

bool Basic_transaction_parser::operator()(MySQL::Binary_log_event_ptr &ev)
{
  switch(ev->get_event_type())
  {
    case MySQL::QUERY_EVENT:
    {
      MySQL::Query_event *qev= static_cast<MySQL::Query_event *>(ev->body());
      if (qev->query == "BEGIN")
      {
        std::cout << "Transaction has started!" << std::endl;
        m_transaction_state= STARTING;
      }
      if (qev->query == "COMMIT")
      {
        std::cout << "Transaction is committed!" << std::endl;
        m_transaction_state= COMMITTING;
      }
    }
    break;
    case MySQL::XID_EVENT:
    {
      /* Transaction capable engines end their transactions with XID */
      std::cout << "Transaction is committed!" << std::endl;
      m_transaction_state= COMMITTING;
    }
    break;
  }

  switch(m_transaction_state)
  {
      case IN_PROGRESS:
      {
        switch (ev->get_event_type())
        {
          case MySQL::WRITE_ROWS_EVENT:
          case MySQL::DELETE_ROWS_EVENT:
          case MySQL::UPDATE_ROWS_EVENT:
          case MySQL::TABLE_MAP_EVENT:
            m_event_stack.push_back(ev);
            return true;
          default:
            return false;
        }
      }
      case STARTING:
      {
        m_transaction_state= IN_PROGRESS;
        m_start_time= ev->header()->timestamp;
        delete ev; // drop the begin event
        return true;
      }
      case COMMITTING:
      {
        delete ev; // drop the commit event
        ev= MySQL::create_transaction_log_event();
        /**
         * Propagate the start time for the transaction to the newly created
         * event.
         */
        ev->header()->timestamp= m_start_time;
        MySQL::Transaction_log_event *trans= static_cast<MySQL::Transaction_log_event*>(ev->body());
          
        //std::cout << "There are " << m_event_stack.size() << " events in the transaction: ";
        std::list <MySQL::Binary_log_event_ptr>::iterator it= m_event_stack.begin();
        while( m_event_stack.size() > 0)
        {
          MySQL::Binary_log_event_ptr event= m_event_stack.front();
          m_event_stack.pop_front();
          switch(event->get_event_type())
          {
            case MySQL::TABLE_MAP_EVENT:
            {
              /*
               Index the table name with a table id to ease lookup later.
              */
              MySQL::Table_map_event *tm= static_cast<MySQL::Table_map_event *>(event->body());
              trans->m_table_map.insert(MySQL::Event_index_element(tm->table_id,event));
              trans->m_events.push_back(event);
            }
            break;
            case MySQL::WRITE_ROWS_EVENT:
            case MySQL::DELETE_ROWS_EVENT:
            case MySQL::UPDATE_ROWS_EVENT:
            {
              trans->m_events.push_back(event);

              /*
               * Propagate last known next position
               */
              ev->header()->next_position= event->header()->next_position;
            }
            break;
            default:
              delete event;

          }
        } // end while
        m_transaction_state= NOT_IN_PROGRESS;
        return false;
      }
      case NOT_IN_PROGRESS:
      default:
        return false;
  } // end switch
  
}


/**
  Print a packed value of the given SQL type into IO cache

  @param[in] field_data        Pointer to string
  @param[in] column_type       Column type
  @param[in] metadata          Column meta information
  @param[out] typestr          SQL type string buffer (for verbose output)
  @param[out] typestr_length   Size of typestr

  @retval   - number of bytes scanned from ptr.
*/

static size_t
log_event_print_value(unsigned char *field_data,
                      unsigned int column_type, unsigned char *metadata_ptr,
                      char *typestr, size_t typestr_length)
{
  unsigned metadata= *((unsigned*)metadata_ptr);
  size_t length= 0; //calc_field_size(column_type, field_data, metadata_ptr);

  MySQL::system::buffer_source field_data_buf((char *)field_data,length);
  switch (column_type) {
  case MySQL::system::MYSQL_TYPE_LONG:
    {
      boost::uint32_t ui;
      MySQL::system::Protocol_chunk<boost::uint32_t> type_long(ui);
      field_data_buf >> type_long;
      return 4;
    }

  case MySQL::system::MYSQL_TYPE_TINY:
    {
      boost::uint8_t ui;
      MySQL::system::Protocol_chunk<boost::uint8_t> type_tiny(ui);
      field_data_buf >> type_tiny;
      return 1;
    }

  case MySQL::system::MYSQL_TYPE_SHORT:
    {
      boost::uint16_t ui;
      MySQL::system::Protocol_chunk<boost::uint16_t> type_short(ui);
      field_data_buf >> type_short;
      return 2;
    }

  case MySQL::system::MYSQL_TYPE_INT24:
    {
     
      return 3;
    }

  case MySQL::system::MYSQL_TYPE_LONGLONG:
    {
      //char tmp[64];
      //longlong si= sint8korr(ptr);
      //longlong10_to_str(si, tmp, -10);
      //my_b_printf(file, "%s", tmp);
      //if (si < 0)
      //{
      //  ulonglong ui= uint8korr(ptr);
      //  longlong10_to_str((longlong) ui, tmp, 10);
      //  my_b_printf(file, " (%s)", tmp);
      //}
      //my_snprintf(typestr, typestr_length, "LONGINT");
      return 8;
    }

  case MySQL::system::MYSQL_TYPE_NEWDECIMAL:
    {
      //uint precision= meta >> 8;
      //uint decimals= meta & 0xFF;
      //uint bin_size= my_decimal_get_binary_size(precision, decimals);
      //my_decimal dec;
      //binary2my_decimal(E_DEC_FATAL_ERROR, (uchar*) ptr, &dec,
      //                  precision, decimals);
      //int i, end;
      //char buff[512], *pos;
      //pos= buff;
      //pos+= my_sprintf(buff, (buff, "%s", dec.sign() ? "-" : ""));
      //end= ROUND_UP(dec.frac) + ROUND_UP(dec.intg)-1;
      //for (i=0; i < end; i++)
      //  pos+= my_sprintf(pos, (pos, "%09d.", dec.buf[i]));
      //pos+= my_sprintf(pos, (pos, "%09d", dec.buf[i]));
      //my_b_printf(file, "%s", buff);
      //my_snprintf(typestr, typestr_length, "DECIMAL(%d,%d)",
      //            precision, decimals);
      //return bin_size;
    }

  case MySQL::system::MYSQL_TYPE_FLOAT:
    {
      //float fl;
      //float4get(fl, ptr);
      //char tmp[320];
      //sprintf(tmp, "%-20g", (double) fl);
      //my_b_printf(file, "%s", tmp); /* my_snprintf doesn't support %-20g */
      //my_snprintf(typestr, typestr_length, "FLOAT");
      return 4;
    }

  case MySQL::system::MYSQL_TYPE_DOUBLE:
    {
      //double dbl;
      //float8get(dbl, ptr);
      //char tmp[320];
      //sprintf(tmp, "%-.20g", dbl); /* my_snprintf doesn't support %-20g */
      //my_b_printf(file, "%s", tmp);
      //strcpy(typestr, "DOUBLE");
      return 8;
    }

  case MySQL::system::MYSQL_TYPE_BIT:
    {
      /* Meta-data: bit_len, bytes_in_rec, 2 bytes */
      //uint nbits= ((meta >> 8) * 8) + (meta & 0xFF);
      //length= (nbits + 7) / 8;
      //my_b_write_bit(file, ptr, nbits);
      //my_snprintf(typestr, typestr_length, "BIT(%d)", nbits);
      //return length;
    }

  case MySQL::system::MYSQL_TYPE_TIMESTAMP:
    {
      //uint32 i32= uint4korr(ptr);
      //my_b_printf(file, "%d", i32);
      //my_snprintf(typestr, typestr_length, "TIMESTAMP");
      //return 4;
    }

  case MySQL::system::MYSQL_TYPE_DATETIME:
    {
      //size_t d, t;
      //uint64 i64= uint8korr(ptr); /* YYYYMMDDhhmmss */
      //d= i64 / 1000000;
      //t= i64 % 1000000;
      //my_b_printf(file, "%04d-%02d-%02d %02d:%02d:%02d",
      //            d / 10000, (d % 10000) / 100, d % 100,
      //            t / 10000, (t % 10000) / 100, t % 100);
      //my_snprintf(typestr, typestr_length, "DATETIME");
      return 8;
    }

  case MySQL::system::MYSQL_TYPE_TIME:
    {
      //uint32 i32= uint3korr(ptr);
      //my_b_printf(file, "'%02d:%02d:%02d'",
      //            i32 / 10000, (i32 % 10000) / 100, i32 % 100);
      //my_snprintf(typestr,  typestr_length, "TIME");
      //return 3;
    }

  case MySQL::system::MYSQL_TYPE_DATE:
    {
      //uint i32= uint3korr(ptr);
      //my_b_printf(file , "'%04d:%02d:%02d'",
      //            (i32 / (16L * 32L)), (i32 / 32L % 16L), (i32 % 32L));
      //my_snprintf(typestr, typestr_length, "DATE");
      //return 3;
    }

  case MySQL::system::MYSQL_TYPE_YEAR:
    {
      //uint32 i32= *ptr;
      //my_b_printf(file, "%04d", i32+ 1900);
      //my_snprintf(typestr, typestr_length, "YEAR");
      //return 1;
    }

  case MySQL::system::MYSQL_TYPE_ENUM:
    //switch (length) {
    //case 1:
    //  my_b_printf(file, "%d", (int) *ptr);
    //  my_snprintf(typestr, typestr_length, "ENUM(1 byte)");
    //  return 1;
    //case 2:
    //  {
    //    int32 i32= uint2korr(ptr);
    //    my_b_printf(file, "%d", i32);
    //    my_snprintf(typestr, typestr_length, "ENUM(2 bytes)");
    //   return 2;
    //  }
    //default:
    //  my_b_printf(file, "!! Unknown ENUM packlen=%d", length);
    //  return 0;
    //}
    break;

  case MySQL::system::MYSQL_TYPE_SET:
    //my_b_write_bit(file, ptr , length * 8);
    //my_snprintf(typestr, typestr_length, "SET(%d bytes)", length);
    //return length;

  case MySQL::system::MYSQL_TYPE_BLOB:
    //switch (meta) {
    //case 1:
    //  length= *ptr;
    //  my_b_write_quoted(file, ptr + 1, length);
    //  my_snprintf(typestr, typestr_length, "TINYBLOB/TINYTEXT");
    //  return length + 1;
    //case 2:
    //  length= uint2korr(ptr);
    //  my_b_write_quoted(file, ptr + 2, length);
    //  my_snprintf(typestr, typestr_length, "BLOB/TEXT");
    //  return length + 2;
    //case 3:
    //  length= uint3korr(ptr);
    //  my_b_write_quoted(file, ptr + 3, length);
    //  my_snprintf(typestr, typestr_length, "MEDIUMBLOB/MEDIUMTEXT");
    //  return length + 3;
    //case 4:
    //  length= uint4korr(ptr);
    //  my_b_write_quoted(file, ptr + 4, length);
    //  my_snprintf(typestr, typestr_length, "LONGBLOB/LONGTEXT");
    //  return length + 4;
    //default:
    //  my_b_printf(file, "!! Unknown BLOB packlen=%d", length);
    //  return 0;
    //}

  case MySQL::system::MYSQL_TYPE_VARCHAR:
  case MySQL::system::MYSQL_TYPE_VAR_STRING:
    //length= meta;
    //my_snprintf(typestr, typestr_length, "VARSTRING(%d)", length);
    //return my_b_write_quoted_with_length(file, ptr, length);

  case MySQL::system::MYSQL_TYPE_STRING:
    //my_snprintf(typestr, typestr_length, "STRING(%d)", length);
    //return my_b_write_quoted_with_length(file, ptr, length);

  default:
    {
      //char tmp[5];
      //my_snprintf(tmp, sizeof(tmp), "%04x", meta);
      //my_b_printf(file,
      //            "!! Don't know how to handle column type=%d meta=%d (%s)",
      //            type, meta, tmp);
    }
    break;
  }
  *typestr= 0;
  return 0;
}


