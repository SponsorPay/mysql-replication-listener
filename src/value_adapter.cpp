#include "value_adapter.h"
#include "binlog_event.h"
#include <boost/lexical_cast.hpp>
#include <iomanip>

using namespace MySQL;
using namespace MySQL::system;
namespace MySQL {

int calc_field_size(unsigned char column_type, unsigned char *field_ptr, boost::uint32_t metadata)
{
  boost::uint32_t length;

  switch (column_type) {
  case MySQL::system::MYSQL_TYPE_VAR_STRING:
    /* This type is hijacked for result set types. */
    length= metadata;
    break;
  case MySQL::system::MYSQL_TYPE_NEWDECIMAL:
    //length= my_decimal_get_binary_size(metadata_ptr[col] >> 8,
    //                                   metadata_ptr[col] & 0xff);
    length= 0;
    break;
  case MySQL::system::MYSQL_TYPE_DECIMAL:
  case MySQL::system::MYSQL_TYPE_FLOAT:
  case MySQL::system::MYSQL_TYPE_DOUBLE:
    length= metadata;
    break;
  /*
    The cases for SET and ENUM are include for completeness, however
    both are mapped to type MYSQL_TYPE_STRING and their real types
    are encoded in the field metadata.
  */
  case MySQL::system::MYSQL_TYPE_SET:
  case MySQL::system::MYSQL_TYPE_ENUM:
  case MySQL::system::MYSQL_TYPE_STRING:
  {
    unsigned char type= metadata >> 8U;
    if ((type == MySQL::system::MYSQL_TYPE_SET) || (type == MySQL::system::MYSQL_TYPE_ENUM))
      length= metadata & 0x00ff;
    else
    {
      /*
        We are reading the actual size from the master_data record
        because this field has the actual lengh stored in the first
        byte.
      */
      length= (unsigned int) *field_ptr+1;
      //DBUG_ASSERT(length != 0);
    }
    break;
  }
  case MySQL::system::MYSQL_TYPE_YEAR:
  case MySQL::system::MYSQL_TYPE_TINY:
    length= 1;
    break;
  case MySQL::system::MYSQL_TYPE_SHORT:
    length= 2;
    break;
  case MySQL::system::MYSQL_TYPE_INT24:
    length= 3;
    break;
  case MySQL::system::MYSQL_TYPE_LONG:
    length= 4;
    break;
//  case MYSQL_TYPE_LONGLONG:
//    length= 8;
//    break;
  case MySQL::system::MYSQL_TYPE_NULL:
    length= 0;
    break;
  case MySQL::system::MYSQL_TYPE_NEWDATE:
    length= 3;
    break;
  case MySQL::system::MYSQL_TYPE_DATE:
  case MySQL::system::MYSQL_TYPE_TIME:
    length= 3;
    break;
  case MySQL::system::MYSQL_TYPE_TIMESTAMP:
    length= 4;
    break;
  case MySQL::system::MYSQL_TYPE_DATETIME:
    length= 8;
    break;
  case MySQL::system::MYSQL_TYPE_BIT:
  {
    /*
      Decode the size of the bit field from the master.
        from_len is the length in bytes from the master
        from_bit_len is the number of extra bits stored in the master record
      If from_bit_len is not 0, add 1 to the length to account for accurate
      number of bytes needed.
    */
	boost::uint32_t from_len= (metadata >> 8U) & 0x00ff;
	boost::uint32_t from_bit_len= metadata & 0x00ff;
    //DBUG_ASSERT(from_bit_len <= 7);
    length= from_len + ((from_bit_len > 0) ? 1 : 0);
    break;
  }
  case MySQL::system::MYSQL_TYPE_VARCHAR:
  {
    length= metadata > 255 ? 2 : 1;
    length+= length == 1 ? (boost::uint32_t) *field_ptr : *((boost::uint16_t *)field_ptr);
    break;
  }
  case MySQL::system::MYSQL_TYPE_TINY_BLOB:
  case MySQL::system::MYSQL_TYPE_MEDIUM_BLOB:
  case MySQL::system::MYSQL_TYPE_LONG_BLOB:
  case MySQL::system::MYSQL_TYPE_BLOB:
  case MySQL::system::MYSQL_TYPE_GEOMETRY:
  {
     switch (metadata)
    {
      case 1:
        length= 1+ (boost::uint32_t) field_ptr[0];
        break;
      case 2:
        length= 2+ (boost::uint32_t) (*(boost::uint16_t *)(field_ptr) & 0xFFFF);
        break;
      case 3:
        // TODO make platform indep.
        length= 3+ (boost::uint32_t) (long) (*((boost::uint32_t *) (field_ptr)) & 0xFFFFFF);
        break;
      case 4:
        // TODO make platform indep.
        length= 4+ (boost::uint32_t) (long) *((boost::uint32_t *) (field_ptr));
        break;
      default:
        length= 0;
        break;
    }
    break;
  }
  default:
    length= ~(boost::uint32_t) 0;
  }
  return length;
}


Value::Value(Value &val)
{
  m_size= val.size();
  m_storage= val.storage();
  m_type= val.type();
  m_metadata= val.metadata();
  m_is_null= val.is_null();
}

Value::Value(const Value& val)
{
  m_size= val.m_size;
  m_storage= val.m_storage;
  m_type= val.m_type;
  m_metadata= val.m_metadata;
  m_is_null= val.m_is_null;
}

Value &Value::operator=(const Value &val)
{
  m_size= val.m_size;
  m_storage= val.m_storage;
  m_type= val.m_type;
  m_metadata= val.m_metadata;
  m_is_null= val.m_is_null;
  return *this;
}

bool Value::operator==(Value &val)
{
  return (m_size == val.m_size) &&
         (m_storage == val.m_storage) &&
         (m_type == val.m_type) &&
         (m_metadata == val.m_metadata);
}

bool Value::operator!=(Value &val)
{
  return !operator==(val);
}

char *Value::sql_string_ptr(unsigned long &size)
{
  if (m_is_null || m_size == 0)
  {
    size= 0;
    return 0;
  }
  /*
   Length encoded; First byte is length of string.
  */
  int metadata_length= m_size > 251 ? 2: 1;
  /*
   Size is length of the character string; not of the entire storage
  */
  size= m_size - metadata_length;
  return m_storage + metadata_length;
}

char *Value::sql_blob(unsigned long &size)
{
  if (m_is_null || m_size == 0)
  {
    size= 0;
    return 0;
  }

  /*
   Size was calculated during construction of the object and only inludes the
   size of the blob data, not the metadata part which also is stored in the
   storage. For blobs this part can be between 1-4 bytes long.
  */
  size= m_size - m_metadata;

  /*
   Adjust the storage pointer with the size of the metadata.
  */
  return m_storage + m_metadata;
}

boost::int32_t Value::sql_integer()
{
  if (m_is_null)
  {
    return 0;
  }
  boost::uint32_t to_int;
  Protocol_chunk<boost::uint32_t> prot_integer(to_int);

  buffer_source buff(m_storage, m_size);
  buff >> prot_integer;
  return to_int;
}

boost::int8_t Value::sql_tinyint()
{
  if (m_is_null)
  {
    return 0;
  }
  boost::int8_t to_int;
  Protocol_chunk<boost::int8_t> prot_integer(to_int);

  buffer_source buff(m_storage, m_size);
  buff >> prot_integer;
  return to_int;
}

boost::int16_t Value::sql_smallint()
{
  if (m_is_null)
  {
    return 0;
  }
  boost::int16_t to_int;
  Protocol_chunk<boost::int16_t> prot_integer(to_int);

  buffer_source buff(m_storage, m_size);
  buff >> prot_integer;
  return to_int;
}

boost::int64_t Value::sql_bigint()
{
  if (m_is_null)
  {
    return 0;
  }
  boost::int64_t to_int;
  Protocol_chunk<boost::int64_t> prot_integer(to_int);

  buffer_source buff(m_storage, m_size);
  buff >> prot_integer;
  return to_int;
}

void Converter::to_string(std::string &str, Value &val)
{
  if (val.is_null())
  {
    str= "(NULL)";
    return;
  }

  switch(val.type())
  {
    case MYSQL_TYPE_DECIMAL:
      str= "not implemented";
      break;
    case MYSQL_TYPE_TINY:
      str= boost::lexical_cast<std::string>(static_cast<int>(val.sql_tinyint()));
      break;
    case MYSQL_TYPE_SHORT:
      str= boost::lexical_cast<std::string>(val.sql_smallint());
      break;
    case MYSQL_TYPE_LONG:
      str= boost::lexical_cast<std::string>(val.sql_integer());
      break;
    case MYSQL_TYPE_FLOAT:
      str= "not implemented";
      break;
    case MYSQL_TYPE_DOUBLE:
      str= boost::lexical_cast<std::string>(val.sql_double());
      break;
    case MYSQL_TYPE_NULL:
      str= "not implemented";
      break;
    case MYSQL_TYPE_TIMESTAMP:
      str= boost::lexical_cast<std::string>((boost::uint32_t)val.sql_integer());
      break;

    case MYSQL_TYPE_LONGLONG:
      str= boost::lexical_cast<std::string>(val.sql_bigint());
      break;
    case MYSQL_TYPE_INT24:
      str= "not implemented";
      break;
    case MYSQL_TYPE_DATE:
      str= "not implemented";
      break;
    case MYSQL_TYPE_DATETIME:
    {
      boost::uint64_t timestamp= val.sql_bigint();
      unsigned long d= timestamp / 1000000;
      unsigned long t= timestamp % 1000000;
      std::ostringstream os;

      os << std::setfill('0') << std::setw(4) << d / 10000
         << std::setw(1) << '-'
         << std::setw(2) << (d % 10000) / 100
         << std::setw(1) << '-'
         << std::setw(2) << d % 100
         << std::setw(1) << ' '
         << std::setw(2) << t / 10000
         << std::setw(1) << ':'
         << std::setw(2) << (t % 10000) / 100
         << std::setw(1) << ':'
         << std::setw(2) << t % 100;
    
      str= os.str();
    }
      break;
    case MYSQL_TYPE_TIME:
      str= "not implemented";
      break;
    case MYSQL_TYPE_YEAR:
      str= "not implemented";
      break;
    case MYSQL_TYPE_NEWDATE:
      str= "not implemented";
      break;
    case MYSQL_TYPE_VARCHAR:
    {
      unsigned long size;
      char *ptr= val.sql_string_ptr(size);
      str.append(ptr, size);
    }
      break;
    case MYSQL_TYPE_VAR_STRING:
    {
      str.append(val.storage(), val.size());
    }
    break;
    case MYSQL_TYPE_STRING:
    {
      unsigned long size;
      char *ptr= val.sql_string_ptr(size);
      str.append(ptr, size);
    }
      break;
    case MYSQL_TYPE_BIT:
      str= "not implemented";
      break;
    case MYSQL_TYPE_NEWDECIMAL:
      str= "not implemented";
      break;
    case MYSQL_TYPE_ENUM:
      str= "not implemented";
      break;
    case MYSQL_TYPE_SET:
      str= "not implemented";
      break;
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
    {
      unsigned long size;
      char *ptr= val.sql_blob(size);
      str.append(ptr, size);
    }
      break;
    case MYSQL_TYPE_GEOMETRY:
      str= "not implemented";
      break;
    default:
      str= "not implemented";
      break;
  }
}


void Converter::to_long(long &out, Value &val)
{
  switch(val.type())
  {
    case MYSQL_TYPE_DECIMAL:
      out= (long)val.sql_decimal();
      break;
    case MYSQL_TYPE_TINY:
      out= val.sql_tinyint();
      break;
    case MYSQL_TYPE_SHORT:
      out= val.sql_smallint();
      break;;
    case MYSQL_TYPE_LONG:
      out= (long)val.sql_integer();
      break;
    case MYSQL_TYPE_FLOAT:
      out= 0;
      break;
    case MYSQL_TYPE_DOUBLE:
      out= (long)val.sql_double();
    case MYSQL_TYPE_NULL:
      out= 0;
      break;
    case MYSQL_TYPE_TIMESTAMP:
      out=(boost::uint32_t)val.sql_integer();
      break;

    case MYSQL_TYPE_LONGLONG:
      out= (long)val.sql_bigint();
      break;
    case MYSQL_TYPE_INT24:
      out= 0;
      break;
    case MYSQL_TYPE_DATE:
      out= 0;
      break;
    case MYSQL_TYPE_TIME:
      out= 0;
      break;
    case MYSQL_TYPE_DATETIME:
      out= (long)val.sql_bigint();
      break;
    case MYSQL_TYPE_YEAR:
      out= 0;
      break;
    case MYSQL_TYPE_NEWDATE:
      out= 0;
      break;
    case MYSQL_TYPE_VARCHAR:
      out= 0;
      break;
    case MYSQL_TYPE_BIT:
      out= 0;
      break;
    case MYSQL_TYPE_NEWDECIMAL:
      out= 0;
      break;
    case MYSQL_TYPE_ENUM:
      out= 0;
      break;
    case MYSQL_TYPE_SET:
      out= 0;
      break;
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
      out= 0;
      break;
    case MYSQL_TYPE_VAR_STRING:
    {
      std::string str;
      str.append(val.storage(), val.size());
      out= boost::lexical_cast<long>(str.c_str());
    }
      break;
    case MYSQL_TYPE_STRING:
      out= 0;
      break;
    case MYSQL_TYPE_GEOMETRY:
      out= 0;
      break;
    default:
      out= 0;
      break;
  }
}


} // end namespace MySQL
