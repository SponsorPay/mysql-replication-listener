#include "field_iterator.h"


static bool is_null(unsigned char *bitmap, int index)
{
  unsigned char *byte= bitmap + (index / 8);
  unsigned bit= 1 << ((index) & 7);
  return ((*byte) & bit) != 0;
}

size_t Row_event_iterator::fields(Row_of_fields& fields_vector )
{
  size_t field_offset= m_field_offset;
  int row_field_col_index= 0;
  std::string nullbits= m_row_event->row.substr(field_offset,m_row_event->null_bits_len).c_str();
  field_offset += m_row_event->null_bits_len;
  for( int col_no=0; col_no < m_table_map->columns_len; ++col_no)
  {
    ++row_field_col_index;
    unsigned int type= m_table_map->columns[col_no]&0xFF;
    boost::uint32_t metadata= extract_metadata(m_table_map, col_no);
    MySQL::Value val((enum MySQL::system::enum_field_types)type,
                     metadata,
                     &m_row_event->row[field_offset]);
    if (is_null((unsigned char *)nullbits.c_str(), col_no ))
    {
      val.is_null(true);
    }
    else
    {
       /*
        If the value is null it is not in the list of values and thus we won't
        increse the offset. TODO what if all values are null?!
       */
      field_offset += val.size();
    }
    fields_vector.push_back(val);
  }
  return field_offset;
}

Row_of_fields Row_event_iterator::operator*()
{ // dereferencing
  Row_of_fields fields_vector;
  /*
   * Remember this offset if we need to increate the row pointer
   */
  m_new_field_offset_calculated= fields(fields_vector);

  return fields_vector;
}

//const Row_of_fields& Row_iterator::operator*() const
//{ // dereferencing
//    return fields();
//}

Row_event_iterator& Row_event_iterator::operator++()
{ // preﬁx
  if (m_field_offset < m_row_event->row_len)
  {
    /*
     * If we requested the fields in a previous operations
     * we also calculated the new offset at the same time.
     */
    if (m_new_field_offset_calculated != 0)
    {
      m_field_offset= m_new_field_offset_calculated;
      //m_field_offset += m_row_event->null_bits_len;
      m_new_field_offset_calculated= 0;
      if (m_field_offset >= m_row_event->row_len)
        m_field_offset= 0;
      return *this;
    }

    /*
     * Advance the field offset to the next row
     */
    int row_field_col_index= 0;
    std::string nullbits= m_row_event->row.substr(m_field_offset,m_row_event->null_bits_len).c_str();
    m_field_offset += m_row_event->null_bits_len;
    for( int col_no=0; col_no < m_table_map->columns_len; ++col_no)
    {
      ++row_field_col_index;
      MySQL::Value val((enum MySQL::system::enum_field_types)m_table_map->columns[col_no],
                       m_table_map->metadata[col_no],
                       &m_row_event->row[m_field_offset]);
      if (!is_null((unsigned char *)nullbits.c_str(), col_no))
      {
        m_field_offset += val.size();
      }
      //fields_vector.add(val);
    }
    
    return *this;
  }

  m_field_offset= 0;
  return *this;
  
}

Row_event_iterator Row_event_iterator::operator++(int)
{ // postﬁx
  Row_event_iterator temp = *this;
  ++*this;
  return temp;
}

bool Row_event_iterator::operator==(const Row_event_iterator& x) const
{
  return m_field_offset == x.m_field_offset;
}

bool Row_event_iterator::operator!=(const Row_event_iterator& x) const
{
  return m_field_offset != x.m_field_offset;
}

//Row_iterator Row_iterator::end() const
//{ return Row_iterator(); }

namespace MySQL
{
boost::uint32_t extract_metadata(Table_map_event *map, int col_no)
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

