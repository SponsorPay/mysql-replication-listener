#include "field_iterator.h"

//Row_iterator Row_iterator::end() const
//{ return Row_iterator(); }

namespace MySQL
{


bool is_null(unsigned char *bitmap, int index)
{
  unsigned char *byte= bitmap + (index / 8);
  unsigned bit= 1 << ((index) & 7);
  return ((*byte) & bit) != 0;
}


boost::uint32_t extract_metadata(const Table_map_event *map, int col_no)
{
  int offset= 0;

  for (int i=0; i < col_no; ++i)
  {
    unsigned int type= (unsigned int)map->columns[i]&0xFF;
    offset += lookup_metadata_field_size((enum MySQL::system::enum_field_types)type);
  }

  boost::uint32_t metadata= 0;
  unsigned int type= (unsigned int)map->columns[col_no]&0xFF;
  switch(lookup_metadata_field_size((enum MySQL::system::enum_field_types)type))
  {
  case 1:
    metadata= map->metadata[offset];
  break;
  case 2:
  {
    unsigned int tmp= ((unsigned int)map->metadata[offset])&0xFF;
    metadata=  static_cast<boost::uint32_t >(tmp);
    tmp= (((unsigned int)map->metadata[offset+1])&0xFF) << 8;
    metadata+= static_cast<boost::uint32_t >(tmp);
  }
  break;
  }
  return metadata;
}

int lookup_metadata_field_size(enum MySQL::system::enum_field_types field_type)
{
  switch(field_type)
  {
    case MySQL::system::MYSQL_TYPE_DOUBLE:
    case MySQL::system::MYSQL_TYPE_FLOAT:
    case MySQL::system::MYSQL_TYPE_BLOB:
    case MySQL::system::MYSQL_TYPE_GEOMETRY:
     return 1;
    case MySQL::system::MYSQL_TYPE_BIT:
    case MySQL::system::MYSQL_TYPE_VARCHAR:
    case MySQL::system::MYSQL_TYPE_NEWDECIMAL:
    case MySQL::system::MYSQL_TYPE_STRING:
    case MySQL::system::MYSQL_TYPE_VAR_STRING:
     return 2;
    case MySQL::system::MYSQL_TYPE_DECIMAL:
    case MySQL::system::MYSQL_TYPE_SET:
    case MySQL::system::MYSQL_TYPE_ENUM:
    case MySQL::system::MYSQL_TYPE_YEAR:
    case MySQL::system::MYSQL_TYPE_TINY:
    case MySQL::system::MYSQL_TYPE_SHORT:
    case MySQL::system::MYSQL_TYPE_INT24:
    case MySQL::system::MYSQL_TYPE_LONG:
    case MySQL::system::MYSQL_TYPE_NULL:
    case MySQL::system::MYSQL_TYPE_NEWDATE:
    case MySQL::system::MYSQL_TYPE_DATE:
    case MySQL::system::MYSQL_TYPE_TIME:
    case MySQL::system::MYSQL_TYPE_TIMESTAMP:
    case MySQL::system::MYSQL_TYPE_DATETIME:
    case MySQL::system::MYSQL_TYPE_TINY_BLOB:
    case MySQL::system::MYSQL_TYPE_MEDIUM_BLOB:
    case MySQL::system::MYSQL_TYPE_LONG_BLOB:
    default:
      return 0;
  }
}

} // end namespace MySQL

