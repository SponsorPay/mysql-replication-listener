/*
Copyright (c) 2012, Oracle and/or its affiliates. All rights
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

#include "table_index.cpp"
#include "table_insert.cpp"
#include "binlog_api.h"
#include <getopt.h>
#include <iostream>
#include <iomanip>
#include <stdlib.h>
#include <string>


using mysql::system::create_transport;
using mysql::Binary_log;
using namespace std;

/*
  The following classes inherit the content_handler class. They accept
  an event, and return NULL, as the event is consumed by the content handler.
  The member function process_event is a dedicated member function for handling
  a particular kind of event. This function can be used to extract data from
  the event and transform it in the required form.
*/

/*
  Incident event logs an out of the ordinary event occuring on the master.
  Since this is an unexpected event, process_event prints the message,
  notifying what may have gone wrong on the server.
*/
class Incident_handler : public mysql::Content_handler
{
public:
  Incident_handler() : mysql::Content_handler() {}

  Binary_log_event *process_event(mysql::Incident_event *incident)
  {
    std::cout << "Event type: "
              << mysql::system::get_event_type_str(incident->get_event_type())
              << " length: " << incident->header()->event_length
              << " next pos: " << incident->header()->next_position
              << std::endl;
    std::cout << "type= "
              << (unsigned)incident->type
              << " message= "
              << incident->message
              <<  std::endl
              <<  std::endl;
    /* Consume the event */
    delete incident;
    return 0;
  }
};

/*
  This class handles a Row_event. The member function, process_event
  parses the event, iterates over it one row at a time, and calls
  table_insert function with the fields iterator, which allows us to
  iterate one fields at a time.
*/
class Applier : public mysql::Content_handler
{
public:
  Applier(Table_index *index, HDFSSchema *mysqltohdfs_obj)
  {
    m_table_index= index;
    m_hdfs_schema= mysqltohdfs_obj;
  }

  mysql::Binary_log_event *process_event(mysql::Row_event *rev)
  {
    int table_id= rev->table_id;
    Int2event_map::iterator ti_it= m_table_index->find(table_id);
    if (ti_it == m_table_index->end ())
    {
      std::cout << "Table id "
                << table_id
                << " was not registered by any preceding table map event."
                << std::endl;
      return rev;
    }
    /*
     Each row event contains multiple rows and fields. The Row_iterator
     allows us to iterate one row at a time.
    */
    mysql::Row_event_set rows(rev, ti_it->second);

    /* Create a fuly qualified table name */
    std::ostringstream os;

    os << ti_it->second->db_name << '/' << ti_it->second->table_name;

    mysql::Row_event_set::iterator it= rows.begin();
    do
    {
      mysql::Row_of_fields fields= *it;
      long int timestamp= rev->header()->timestamp;
      if (rev->get_event_type() == mysql::WRITE_ROWS_EVENT)
        table_insert(os.str(), fields, timestamp, m_hdfs_schema);
    } while (++it != rows.end());

    /* Consume the event */
    delete rev;
    return 0;
   }
private:
  Table_index *m_table_index;
  HDFSSchema *m_hdfs_schema;
};

/* Set the default values for delimiters */
string field_delim="\x01";
string row_delim="\x0A";

static struct option long_options[]=
{
  /* These options take an argument */
  {"fields_delimited_by", required_argument, 0, 'f'},
  {"row_delimited_by", required_argument, 0, 'r'},
  {0, 0, 0, 0}
};

void usage(const struct option *options)
{
  const struct option *optp;
  cerr <<  "Usage:\n\nmain [optios] MySQL_URL HDFS_URL \n\n"
       <<  "Example:\n\n"
       <<  "./main [options] mysql://root@127.0.0.1:3306 "
       <<  "hdfs://localhost:9000\n\n";

  for (optp= options; optp->name; optp++)
  {
    cerr << "-" << (char)optp->val
         << (strlen(optp->name) ? ", " : "  ")
         << "--" << optp->name;

    if (optp->has_arg  == 1)
      cerr << "=hex_arg Ex: 0xnn";
   }
}

/* Parse the arguments */

void parse_args(int *argc, char **argv)
{
  char c;
  string as;
  ostringstream s;

  while (true)
  {
    /* getopt_long stores the option index here */
    int option_index= 0;
    int c= getopt_long(*argc, argv, "r:f:", long_options, &option_index);
    /* Detect the end of options */
    if (c == -1)
      break;
    /*
       Using stringstream to convert hexadecimal delimiters taken as input,
       to a single character.
    */
    int delim;
    std::stringstream delim_str;

    switch (c)
    {
    case 'f':
      delim_str << hex << optarg;
      delim_str >> delim;
      if (delim == 0 && optarg != "0x00")
      {
        cerr << "Incorrect hexadecimal format used" << endl;
        usage(long_options);
        exit(2);
      }
      field_delim= (char)delim;
      break;
    case 'r':
      delim_str << hex << optarg;
      delim_str >> delim;
      if (delim == 0 && optarg != "0x00")
      {
        cerr << "Incorrect hexadecimal format used" << endl;
        usage(long_options);
        exit(2);
      }
      row_delim= (char)delim;
      break;
    case '?':
    default:
      /* getopt has already printed out the error */
      usage(long_options);
      exit(2);
    } // end of switch
  } // end of while loop

  if (optind == *argc || *argc - optind != 2)
  {
    usage(long_options);
    exit(2);
  }
  else
  {
    *argc= optind;
  }
} // end parse_args


void parse_hdfs_url(const char *body, size_t len, string &hdfs_host, unsigned int *hdfs_port)
{

  if (strncmp(body, "hdfs://", 7) != 0)
  {
    usage(long_options);
    exit(2);
  }

  const char *host = body+7;
  const char *host_end = strchr(host, ':');
  if (host == host_end)
  {
    usage(long_options);
    exit(2);
  } // No hostname was found.

  /*
    If no ':' was found there is no port, so the host ends at the end
    of the string.
  */

  if (host_end == 0)
    host_end = body + len;

  /* Find the port number */
  unsigned long portno = 9100;

  if (*host_end == ':')
    portno = strtoul(host_end + 1, NULL, 10);

  hdfs_host= std::string(host, host_end - host);
  *hdfs_port= portno;
  /*
    Host name is now the string [host, port-1) if port != NULL and
    [host, EOS) otherwise.
    Port number is stored in portno, either the default, or a parsed one.
  */
}

int main(int argc, char** argv)
{
  string mysql_uri, hdfs_uri;
  string hdfs_host;
  unsigned int hdfs_port;

  parse_args(&argc, argv);

  mysql_uri= argv[argc++];
  hdfs_uri= argv[argc++];

  parse_hdfs_url(hdfs_uri.c_str(), strlen(hdfs_uri.c_str()),
                 hdfs_host, &hdfs_port);

  Binary_log binlog(create_transport(mysql_uri.c_str()));

  try
  {
    HDFSSchema sqltohdfs_obj(hdfs_host, hdfs_port);

    /*
      Attach a custom event content handlers
    */
    Incident_handler incident_hndlr;
    Table_index table_event_hdlr;
    Applier replay_hndlr(&table_event_hdlr, &sqltohdfs_obj);

    sqltohdfs_obj.hdfs_field_delim= field_delim;
    sqltohdfs_obj.hdfs_row_delim= row_delim;

    /*
      The content handlers are registered using content_handler_pipeline.
      Each content handler is pushed onto a stack and it is possible for
      one handler to rewrite or create new events for the proceeding handlers
      on the stack to act on. conent_handler_pipeline pushes the event to the
      right function when the event happens.
    */
    binlog.content_handler_pipeline()->push_back(&table_event_hdlr);
    binlog.content_handler_pipeline()->push_back(&incident_hndlr);
    binlog.content_handler_pipeline()->push_back(&replay_hndlr);

    if (binlog.connect())
    {
      fprintf(stderr,"Can't connect to the master.\n");
      return (EXIT_FAILURE);
    }

    while (true)
    {
      /*
        Pull events from the master. This is the heart beat of the event listener.
      */
      Binary_log_event  *event;
      binlog.wait_for_next_event(&event);
      delete event;
    } // end loop
  }
  catch(std::runtime_error& e)
  {
    cout <<  e.what() << endl;
  }
  return (EXIT_SUCCESS);
}
