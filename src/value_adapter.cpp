#include "value_adapter.h"
#include "binlog_event.h"
#include <boost/lexical_cast.hpp>

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
        length= 3+ (boost::uint32_t) (long) (*((unsigned int *) (field_ptr)) & 0xFFFFFF);
        break;
      case 4:
        // TODO make platform indep.
        length= 4+ (boost::uint32_t) (long) *((unsigned int *) (field_ptr));
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

char *Value::sql_string_ptr(size_t &size)
{
  if (m_is_null)
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

char *Value::sql_blob(size_t &size)
{
  if (m_is_null)
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

boost::uint32_t Value::sql_integer()
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

boost::uint64_t Value::sql_long()
{
  if (m_is_null)
  {
    return 0;
  }
  boost::uint64_t to_int;
  Protocol_chunk<boost::uint64_t> prot_integer(to_int);

  buffer_source buff(m_storage, m_size);
  buff >> prot_integer;
  return to_int;
}

boost::any Converter::to_any()
{
  boost::any value;
  switch(m_val.type())
  {
    case MYSQL_TYPE_DECIMAL:
      break;
    case MYSQL_TYPE_TINY:
      break;
    case MYSQL_TYPE_SHORT:
      break;
    case MYSQL_TYPE_LONG:
      value= m_val.sql_long();
      break;
    case MYSQL_TYPE_FLOAT:
      break;
    case MYSQL_TYPE_DOUBLE:
      value= m_val.sql_double();
    case MYSQL_TYPE_NULL:
      break;
    case MYSQL_TYPE_TIMESTAMP:
      break;
    case MYSQL_TYPE_LONGLONG:
      value= m_val.sql_long();
      break;
    case MYSQL_TYPE_INT24:
      break;
    case MYSQL_TYPE_DATE:
      break;
    case MYSQL_TYPE_TIME:
      break;
    case MYSQL_TYPE_DATETIME:
      break;
    case MYSQL_TYPE_YEAR:
      break;
    case MYSQL_TYPE_NEWDATE:
      break;
    case MYSQL_TYPE_VARCHAR:
      break;
    case MYSQL_TYPE_BIT:
      break;
    case MYSQL_TYPE_NEWDECIMAL:
      break;
    case MYSQL_TYPE_ENUM:
      break;
    case MYSQL_TYPE_SET:
      break;
    case MYSQL_TYPE_TINY_BLOB:
      break;
    case MYSQL_TYPE_MEDIUM_BLOB:
      break;
    case MYSQL_TYPE_LONG_BLOB:
      break;
    case MYSQL_TYPE_BLOB:
      break;
    case MYSQL_TYPE_VAR_STRING:
      break;
    case MYSQL_TYPE_STRING:
      break;
    case MYSQL_TYPE_GEOMETRY:
      break;
    default:
      value= 0;
  }
  return value;
}

void Converter::to_string(std::string &str)
{
  switch(m_val.type())
  {
    case MYSQL_TYPE_DECIMAL:
      str= "not implemented";
      break;
    case MYSQL_TYPE_TINY:
      str= "not implemented";
      break;
    case MYSQL_TYPE_SHORT:
      str= "not implemented";
      break;;
    case MYSQL_TYPE_LONG:
      if (m_val.is_null())
        str= "(NULL)";
      else
        str= boost::lexical_cast<std::string>(m_val.sql_integer());
      break;
    case MYSQL_TYPE_FLOAT:
      str= "not implemented";
      break;
    case MYSQL_TYPE_DOUBLE:
      if (m_val.is_null())
        str= "(NULL)";
      else
        str= boost::lexical_cast<std::string>(m_val.sql_double());
    case MYSQL_TYPE_NULL:
      str= "not implemented";
      break;
    case MYSQL_TYPE_TIMESTAMP:
      str="not implemented";
      break;

    case MYSQL_TYPE_LONGLONG:
      if (m_val.is_null())
        str= "(NULL)";
      else
        str= boost::lexical_cast<std::string>(m_val.sql_long());
      break;
    case MYSQL_TYPE_INT24:
      str= "not implemented";
      break;
    case MYSQL_TYPE_DATE:
      str= "not implemented";
      break;
    case MYSQL_TYPE_TIME:
      str= "not implemented";
      break;
    case MYSQL_TYPE_DATETIME:
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
      if (m_val.is_null())
        str= "(NULL)";
      else
      {
        size_t size;
        char *ptr= m_val.sql_string_ptr(size);
        str.append(ptr, size);
      }
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
      if (m_val.is_null())
        str= "(NULL)";
      else
      {
        size_t size;
        char *ptr= m_val.sql_blob(size);
        str.append(ptr, size);
      }
    }
      break;
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
      if (m_val.is_null())
        str= "(NULL)";
      else
        str.append(m_val.storage(), m_val.size());
      break;
    case MYSQL_TYPE_GEOMETRY:
      str= "not implemented";
      break;
    default:
      str= "not implemented";
      break;
  }
}


void Converter::to_long(unsigned long &val)
{
  switch(m_val.type())
  {
    case MYSQL_TYPE_DECIMAL:
      val= 0;
      break;
    case MYSQL_TYPE_TINY:
      val= 0;
      break;
    case MYSQL_TYPE_SHORT:
      val= 0;
      break;;
    case MYSQL_TYPE_LONG:
      val= (unsigned long)m_val.sql_integer();
      break;
    case MYSQL_TYPE_FLOAT:
      val= 0;
      break;
    case MYSQL_TYPE_DOUBLE:
      val= (unsigned long)m_val.sql_double();
    case MYSQL_TYPE_NULL:
      val= 0;
      break;
    case MYSQL_TYPE_TIMESTAMP:
      val=0;
      break;

    case MYSQL_TYPE_LONGLONG:
      val= m_val.sql_long();
      break;
    case MYSQL_TYPE_INT24:
      val= 0;
      break;
    case MYSQL_TYPE_DATE:
      val= 0;
      break;
    case MYSQL_TYPE_TIME:
      val= 0;
      break;
    case MYSQL_TYPE_DATETIME:
      val= 0;
      break;
    case MYSQL_TYPE_YEAR:
      val= 0;
      break;
    case MYSQL_TYPE_NEWDATE:
      val= 0;
      break;
    case MYSQL_TYPE_VARCHAR:
      val= 0;
      break;
    case MYSQL_TYPE_BIT:
      val= 0;
      break;
    case MYSQL_TYPE_NEWDECIMAL:
      val= 0;
      break;
    case MYSQL_TYPE_ENUM:
      val= 0;
      break;
    case MYSQL_TYPE_SET:
      val= 0;
      break;
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
      val= 0;
      break;
    case MYSQL_TYPE_VAR_STRING:
    {
      std::string str;
      str.append(m_val.storage(), m_val.size());
      val= boost::lexical_cast<unsigned long>(str.c_str());
    }
      break;
    case MYSQL_TYPE_STRING:
      val= 0;
      break;
    case MYSQL_TYPE_GEOMETRY:
      val= 0;
      break;
    default:
      val= 0;
      break;
  }
}


} // end namespace MySQL
