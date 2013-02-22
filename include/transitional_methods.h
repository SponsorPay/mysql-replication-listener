/*
Copyright (c) 2003, 2011, 2013, Oracle and/or its affiliates. All rights
reserved.

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; version 2 of
the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
02110-1301  USA
*/

#include <cstring>
#ifdef min //definition of min() and max() in std and libmysqlclient 
           //can be/are different
#undef min
#endif
#ifdef max
#undef max
#endif
/**
  Copies the first length character from source to destination

  @param destination  pointer to the destination where the content
                      is to be copied
  @param source       C string to be copied
  
  @param length       length of the characters to be copied from source
  
  @retval             destination is returned 
*/

uchar *net_store_data(uchar *destination, const uchar *source, size_t length)
{
  destination= net_store_length(destination, length);
  memcpy(destination, source, length);
  return destination + length;
}
