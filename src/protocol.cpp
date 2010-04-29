#include "protocol.h"
using namespace MySQL;
using namespace MySQL::system;

namespace MySQL { namespace system {
  
int proto_read_package_header(tcp::socket *socket, unsigned long *packet_length, unsigned char *packet_no)
{
  unsigned char buf[4];

  try {
    boost::asio::read(*socket, boost::asio::buffer(buf, 4),
                      boost::asio::transfer_at_least(4));
  } catch (boost::system::system_error e)
  {
    return 1;
  }
  *packet_length= (unsigned long)(buf[0] &0xFF);
  *packet_length+= (unsigned long)((buf[1] &0xFF)<<8);
  *packet_length+= (unsigned long)((buf[2] &0xFF)<<16);
  *packet_no= (unsigned char)buf[3];
  return 0;
}

int proto_read_package_header(tcp::socket *socket, boost::asio::streambuf &buff, unsigned long *packet_length, unsigned char *packet_no)
{
  std::streamsize inbuff= buff.in_avail();
  if( inbuff < 0)
    inbuff= 0;

  if (4 > inbuff)
  {
    try {
      boost::asio::read(*socket, buff,
                        boost::asio::transfer_at_least(4-inbuff));
    } catch (boost::system::system_error e)
    {
      return 1;
    }
  }
  char ch;
  std::istream is(&buff);
  is.get(ch);
  *packet_length= (unsigned long)ch;
  is.get(ch);
  *packet_length+= (unsigned long)(ch<<8);
  is.get(ch);
  *packet_length+= (unsigned long)(ch<<16);
  is.get(ch);
  *packet_no= (unsigned char)ch;
  return 0;
}


int proto_get_one_package(tcp::socket *socket, boost::asio::streambuf &buff, boost::uint8_t *packet_no)
{
  unsigned long packet_length;
  if (proto_read_package_header(socket, buff, &packet_length, packet_no))
      return 0;
  std::streamsize inbuffer= buff.in_avail();
  if (inbuffer < 0)
    inbuffer= 0;
  if (packet_length > inbuffer)
    boost::asio::read(*socket, buff, boost::asio::transfer_at_least(packet_length-inbuffer));

  return packet_length;
}

void prot_parse_error_message(std::istream &is, struct st_error_package &err, int packet_length)
{
    boost::uint8_t marker;

    Protocol_chunk<boost::uint16_t> prot_errno(err.error_code);
    Protocol_chunk<boost::uint8_t>  prot_marker(marker);
    Protocol_chunk<boost::uint8_t>  prot_sql_state(err.sql_state,5);

    is >> prot_errno
       >> prot_marker
       >> prot_sql_state;

    // TODO is the number of bytes read = is.tellg() ?

    int message_size= packet_length -2 -1 -5; // the remaining part of the package
    if (message_size > 0)
    {
      char *buf= new char[message_size+1];
      err.message= (boost::uint8_t *)&buf[0];
    }

    is >> err.message;

    if (message_size > 0)
    {
      Protocol_chunk<boost::uint8_t> prot_message(err.message, message_size);
      is >> prot_message;
      err.message[message_size]= '\0';
    }
};

void prot_parse_ok_message(std::istream &is, struct st_ok_package &ok, int packet_length)
{
 // TODO: Assure that zero length messages can be but on the input stream.

    ok.message= 0;
  //Protocol_chunk<boost::uint8_t>  prot_result_type(result_type);
  Protocol_chunk<boost::uint64_t> prot_affected_rows(ok.affected_rows);
  Protocol_chunk<boost::uint64_t> prot_insert_id(ok.insert_id);
  Protocol_chunk<boost::uint16_t> prot_server_status(ok.server_status);
  Protocol_chunk<boost::uint16_t> prot_warning_count(ok.warning_count);


  int message_size= packet_length - 2- prot_affected_rows.size() - prot_insert_id.size() - prot_server_status.size()- prot_warning_count.size();
  if (message_size > 0)
  {
    char *buf= new char[message_size+1];
    ok.message= (boost::uint8_t *)&buf[0];
  }

  Protocol_chunk<boost::uint8_t>  prot_message(ok.message, message_size);

  prot_affected_rows.set_length_encoded_binary(true);
  prot_insert_id.set_length_encoded_binary(true);

   is  >> prot_affected_rows
       >> prot_insert_id
       >> prot_server_status
       >> prot_warning_count;

   if (message_size > 0)
   {
     is >> prot_message;
     ok.message[message_size]= '\0';
   }
}

void prot_parse_eof_message(std::istream &is, struct st_eof_package &eof)
{

  Protocol_chunk<boost::uint16_t> proto_warning_count(eof.warning_count);
  Protocol_chunk<boost::uint16_t> proto_status_flags(eof.status_flags);

  is >> proto_warning_count
     >> proto_status_flags;
};

void proto_get_handshake_package(std::istream &is, struct st_handshake_package &p, int packet_length)
{
  boost::uint8_t filler;
  boost::uint8_t filler2[13];

  Protocol_chunk<boost::uint8_t>   proto_protocol_version(p.protocol_version);
  Protocol_chunk<boost::uint32_t>  proto_thread_id(p.thread_id);
  Protocol_chunk<boost::uint8_t>   proto_scramble_buffer(p.scramble_buff, 8);
  Protocol_chunk<boost::uint8_t>   proto_filler(filler);
  Protocol_chunk<boost::uint16_t>  proto_server_capabilities(p.server_capabilities);
  Protocol_chunk<boost::uint8_t>   proto_server_language(p.server_language);
  Protocol_chunk<boost::uint16_t>  proto_server_status(p.server_status);
  Protocol_chunk<boost::uint8_t>   proto_filler2(filler2,13);
  Protocol_chunk<boost::uint8_t>   proto_scramble_buffer2(p.scramble_buff2, 13);

  is  >> proto_protocol_version
      >> p.server_version_str
      >> proto_thread_id
      >> proto_scramble_buffer
      >> proto_filler
      >> proto_server_capabilities
      >> proto_server_language
      >> proto_server_status
      >> proto_filler2
      >> proto_scramble_buffer2;

  assert(filler == 0);

}

void write_packet_header(char *buff, boost::uint16_t size, boost::uint8_t packet_no)
{
  int3store(buff, size);
  buff[3]= (char)packet_no;
}


buffer_source &operator>>(buffer_source &src, Protocol &chunk)
{
  char ch;
  int ct= 0;
  char *ptr= (char*)chunk.data();

  while(ct < chunk.size() && src.m_ptr < src.m_size)
  {
      ptr[ct]= src.m_src[src.m_ptr];
      ++ct;
      ++src.m_ptr;
  }
  return src;
}

std::istream &operator>>(std::istream &is, Protocol &chunk)
{
 if (chunk.is_length_encoded_binary())
  {
      int ct= 0;
      is.read((char *)chunk.data(),1);
      unsigned char byte= *(unsigned char *)chunk.data();
      if (byte < 250)
      {
          chunk.collapse_size(1);
          return is;
      }
      else if (byte == 251)
      {
        // is this a row data packet? if so, then this column value is NULL
          chunk.collapse_size(1);
          ct= 1;
      }
      else if (byte == 252)
      {
        chunk.collapse_size(2);
        ct= 1;
      }
      else if(byte == 253)
      {
        chunk.collapse_size(3);
        ct= 1;
      }

      /* Read remaining bytes */
      //is.read((char *)chunk.data(), chunk.size()-1);
      char ch;
      char *ptr= (char*)chunk.data();
      while(ct < chunk.size())
      {
         is.get(ch);
         ptr[ct]= ch;
         ++ct;
      }
  }
  else
  {
     char ch;
     int ct= 0;
     char *ptr= (char*)chunk.data();
     int sz= chunk.size();
     while(ct < sz)
     {
         is.get(ch);
         ptr[ct]= ch;
         ++ct;
     }
  }

  return is;
}

std::istream &operator>>(std::istream &is, std::string &str)
{
    std::ostringstream out;
    char ch;
    int ct= 0;
    do
    {
      is.get(ch);
      out.put(ch);
      ++ct;
    } while (is.good() && ch != '\0');
    str.append(out.str());
    return is;
}

std::istream &operator>>(std::istream &is, Protocol_chunk_string_len &lenstr)
{
    boost::uint8_t len;
    Protocol_chunk<boost::uint8_t> proto_str_len(len);
    is >> proto_str_len;
    Protocol_chunk_string   proto_str(*lenstr.m_storage, len);
    is >> proto_str;
    return is;
}

std::ostream &operator<<(std::ostream &os, Protocol &chunk)
{
  if (!os.bad())
    os.write((const char *) chunk.data(),chunk.size());
  return os;
}


} } // end namespace MySQL::system

