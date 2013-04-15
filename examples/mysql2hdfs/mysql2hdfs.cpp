/*
Copyright (c) 2013, Oracle and/or its affiliates. All rights
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

#include "table_index.h"
#include "table_insert.h"
#include "binlog_api.h"
#include <getopt.h>
#include <iostream>
#include <iomanip>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <stdexcept>

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
              << std::endl
              << std::endl;
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
      std::cerr << "Table id "
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

    string table_name= ti_it->second->table_name;
    string db_name= ti_it->second->db_name;
    try
    {
      mysql::Row_event_set::iterator it= rows.begin();
      do
      {
        mysql::Row_of_fields fields= *it;
        long int timestamp= rev->header()->timestamp;
        if (rev->get_event_type() == mysql::WRITE_ROWS_EVENT ||
            rev->get_event_type() == mysql::WRITE_ROWS_EVENT_V1)
          table_insert(db_name, table_name, fields, timestamp, m_hdfs_schema);
      } while (++it != rows.end());
    }
    catch (const std::logic_error& le)
    {
      std::cerr << "MySQL Data Type error: " << le.what() << '\n';
    }
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

std::vector<long int> field_index;
std::map<std::string, tname_vec> dbname2tbname_map;
std::vector<std::string> opt_db_name;

/* The structure my_option contains additional description
   to long option name */
struct my_option{
  const char *name;
  int has_arg;
  int *flag;
  int val;
  std::string arg;
  std::string description;
};

static struct my_option my_long_options[]=
{
  {"field-delimiter", required_argument, 0, 'r', "DELIM",
   "Use DELIM instead of ctrl-A for field delimiter"},
  {"row-delimiter", required_argument, 0, 'w', "DELIM",
   "Use DELIM instead of LINE FEED for row delimiter"},
  {"databases", required_argument, 0, 'd',"DB_LIST", "Import entries for"
   " just these databases, optionally include only specified tables."},
  {"fields", required_argument, 0, 'f', "LIST",
   "\tImport entries for just these fields "},
  {"help", no_argument, 0, 'h', "", "Display help"},
  {0, 0, 0, 0}
};

/* long_options structure is used by getopt_long */
static struct option
 long_options[sizeof(my_long_options)/sizeof(my_long_options[0])];

void usage(const struct my_option *options)
{
  const struct my_option *optp;

  cerr <<  "Usage:\n\nhapplier [options] [MySQL_URL] [HDFS_URL] \n\n"
       <<  "Example:\n\n"
       <<  "./happlier [options] mysql://root@127.0.0.1:3306 "
       <<  "hdfs://localhost:9000\n\n";

  for (optp= options; optp->name; optp++)
  {
    cerr << setw(2) << right
         << "-" << (char)optp->val
         << (strlen(optp->name) ? ", " : "  ")
         << setw(2) << right
         << "--" << optp->name;

    if (optp->has_arg  == 1)
      cerr << "=" << optp->arg;
    cerr << "\t"
         << optp->description << endl;
   }

   cerr << "\nDELIM can be a string or an ASCII value in the format '\\nnn'\n"
        << "Escape sequences are not allowed.\n";

   cerr << "\nDB-LIST is made up of one database name, "
        << " or many names separated by commas.\n"
        << "Each database name can be optionally followed by table names.\n"
        << "The table names must follow the database name, "
        << " separated by HYPHENS\n\n"
        << "Example: -d=db_name1-table1-table2,dbname2-table1,dbname3\n";

   cerr << "LIST is made up of one range, or many ranges separated by commas."
        << "Each range is one of:\n"
        <<  " N     N'th byte, character or field, counted from 1\n"
        << " N-    from N'th byte, character or field, to end of line\n"
        << " N-M   from N'th to M'th (included) byte, character or field\n"
        << "-M    from first to M'th (included) byte, character or field\n";
}


bool check_conversion(const char* optarg, long int *position)
{
  char *endp;

  errno= 0;
  *position= strtol(optarg, &endp, 10);
  if ((errno == ERANGE &&
     (*position == LONG_MAX || *position == LONG_MIN)) ||
     (errno == EINVAL) ||
     (errno != 0 && *position == 0) || *endp != '\0' )
  {
    return false;
  }
  return true;
}

/* Parse the arguments */
void parse_args(int *argc, char **argv, std::string* mysql_uri,
                                         std::string* hdfs_uri)
{
  /* Create an array of structure 'option' for the sake of getopt_long */
  for (int i= 0; i < (sizeof(my_long_options)/sizeof(my_long_options[0])); i++)
  {
    long_options[i].name= (my_long_options[i]).name;
    long_options[i].has_arg=(my_long_options[i]).has_arg;
    long_options[i].flag=(my_long_options[i]).flag;
    long_options[i].val= (my_long_options[i]).val;
  }

  string optarg_str, delim_str;
  long int delim_int;
  while (true)
  {
    /* getopt_long stores the option index here */
    int option_index= 0;
    int c= getopt_long(*argc, argv, "r:d:f:w:h", long_options, &option_index);
    /* Detect the end of options */
    if (c == -1)
      break;

    switch (c)
    {
    case 'r':                                   // field-delimiter
      optarg_str= optarg;
      /* Check if the delimiter is a non printable character */
      if (optarg[0] == '\\')
      {
        delim_str= optarg_str.substr(1);
        if (!check_conversion(delim_str.c_str(), &delim_int))
        {
          cerr << "Delimiter must be a string or ascii value in format '\\nnn'"
               << endl;
          usage(my_long_options);
          exit(2);
        }
        field_delim= (char)delim_int;
      }
      /* The delimiter is a string, and requires no conversion. */
      else
        field_delim= optarg;
      break;
    case 'w':                                   // row-delimiter
      optarg_str= optarg;
      /* Check if the delimiter is a non printable cahracter */
      if (optarg[0]== '\\')
      {
        delim_str= optarg_str.substr(1);
        if (!check_conversion(delim_str.c_str(), &delim_int))
        {
          cerr << "invalid field list" << endl;
          usage(my_long_options);
          exit(2);
        }
        row_delim= (char)delim_int;
      }
      /* The delimiter is a string, and requires no conversion. */
      else
        row_delim= optarg;
      break;
    case 'f':                                   // field index
    {
      /*
        The user can specify option as -f=2,6-8,11-
        These are the fields which the user wants to import. It is stored
        in the vector as 2,6,7,8. 11 is assigned to a variable, opt_field_min.
      */
      string word, elem1, elem2;
      stringstream stream(optarg);
      while (getline(stream, word, ','))
      {
        vector<long int>::iterator it= field_index.begin();
        /*
           Find the index numbers specified in the form N-M,i.e.
           include N'th to M'th (included) field. The field indexes are stored
           in a vector. N-M is stored as (M-N + 1) different numbers
           in the vector.
        */
        size_t found= word.find("-");

        if (found != string::npos)
        {
          elem1= word.substr(0, found);
          elem2= word.substr((found+1));
          long int el1, el2;
          if (!check_conversion(elem1.c_str(), &el1) ||
              !check_conversion(elem2.c_str(), &el2) ||
              el1 >= el2 && el1 != 0 && el2 != 0)
          {
            cerr << "invalid field list" << endl;
            usage(my_long_options);
            exit(2);
          }
          // Entries in the form of N-M is found.
          if (!elem1.empty() && !elem2.empty())
          {
            for (int i= el1; i <= el2; i++)
              it= field_index.insert(it, i);
          }
          // Entries in the form of N- or -M is found
          else
          {
            if (!elem1.empty())
              opt_field_min= el1;
            else
              opt_field_max= el2;
          }
        }
        // Entries is in the form of N, only one number.
        else
        {
          long int int_str;
          if (!check_conversion(word.c_str(), &int_str))
          {
            cerr << word << "is not an integer" << endl;
            usage(my_long_options);
            exit(2);
          }
          if (int_str == 0)
          {
            cerr << "Fields are numbered from 1" << endl;
            usage(my_long_options);
            exit(2);
          }
          it= field_index.insert(it, int_str);
        }
      } // end of inner while loop
      opt_field_flag= 1;
      break;
    }
    case 'd':
    {
      /*
        The user can specify this parameter as
        -d=db1-t1-t2,db2-t3-t4-t5, ...
        The information is stored in a map where key is the db_name
        and the value is a vector of table names in that database as given
        by the user. FOr ex: Here the map would contain
           <key>db1  <value> (t1,t2)
           <key>db2  <value> (t3,t4,t5)
           ...
      */
      string db_str, db_name;
      stringstream stream(optarg);
      tname_vec vec1;
      tname_vec::iterator tn_it= vec1.begin();
      while (getline(stream, db_str, ','))
      {
        if (!vec1.empty())
         vec1.clear();
        size_t found= db_str.find("-");
        /* Database name is followed by table names. */
        if (found != string::npos)
        {
          string tb_names, tname;
          db_name= db_str.substr(0, found);
          tb_names= db_str.substr(found+1);
          stringstream db_stream(tb_names);

          while (getline(db_stream, tname, '-'))
            vec1.push_back(tname);
          dbname2tbname_map.insert(opt_dname_element(db_name, vec1));
        }
        /* All tables must be included. */
        else
        {
          db_name= db_str;
          dbname2tbname_map.insert(opt_dname_element(db_name, vec1));
        }
      }
      opt_db_flag= 1;
      break;
    }
    case 'h':
      usage(my_long_options);
      exit(2);
    case '?':
    default:
      /* getopt has already printed out the error */
      usage(my_long_options);
      exit(2);
    } // end of switch
  } // end of while loop

  if (*argc == optind || *argc - optind == 1)
  {
    if (*argc - optind ==1) //one of the uri is provided
    {
      if (strncmp(argv[optind], "hdfs://", 7) == 0)  //the uri is hdfs uri
      {
         *hdfs_uri= argv[optind];
         *mysql_uri= "mysql://user@127.0.0.1:3306";
      }
      else if (strncmp(argv[optind], "mysql://", 8) == 0 ||
               strncmp(argv[optind], "file://", 7) == 0)
      {
         *mysql_uri= argv[optind];
         *hdfs_uri= "hdfs://default:0";
      }
      else
      {
        usage(my_long_options);
        exit(2);
      }
    }
    if (*argc == optind)  //neither of the uri is provided
    {
      *mysql_uri= "mysql://user@127.0.0.1:3306";
      *hdfs_uri= "hdfs://default:0";
    }
  }
  else if (*argc - optind == 2)
  {
    *argc= optind;
    *mysql_uri= argv[optind];
    *hdfs_uri= argv[optind+1];
  }
  else
  {
    usage(my_long_options);
    exit(2);
  }
} // end parse_args


void parse_hdfs_url(const char *body, size_t len, string* hdfs_host,
                          string* hdfs_user, unsigned int *hdfs_port)
{

  if (strncmp(body, "hdfs://", 7) != 0)
  {
    usage(my_long_options);
    exit(2);
  }

  const char *user = body + 7;

  const char *user_end= strpbrk(user, ":@");
  if (*user_end == ':' || user_end == user)
    hdfs_user->clear();
  else
    hdfs_user->assign(user, user_end - user);
  const char *host = *user_end == '@' ? user_end + 1 : user;
  const char *host_end = strchr(host, ':');

  if (host == host_end)
  {
    cerr << "HDFS host name is required"
         << endl;
    usage(my_long_options);
    exit(2);
  } // No hostname was found.

  /*
    If no ':' was found there is no port, so the host ends at the end
    of the string.
  */
  if (host_end == 0)
    host_end = body + len;

  /* Find the port number */
  unsigned long portno = 0;

  if (*host_end == ':')
    portno = strtoul(host_end + 1, NULL, 10);

  hdfs_host->assign(host, host_end - host);
  *hdfs_port= portno;
  /*
    Host name is now the string [host, port-1) if port != NULL and
    [host, EOS) otherwise.
    Port number is stored in portno, either the default, or a parsed one.
    User name is now the string [user,host-1).
  */
}

int main(int argc, char** argv)
{
  string mysql_uri, hdfs_uri;
  string hdfs_host, hdfs_user;
  string data_dir_path;
  unsigned int hdfs_port;
  char type;

  parse_args(&argc, argv, &mysql_uri, &hdfs_uri);
  parse_hdfs_url(hdfs_uri.c_str(), strlen(hdfs_uri.c_str()),
                 &hdfs_host, &hdfs_user, &hdfs_port);

  Binary_log binlog(create_transport(mysql_uri.c_str()));
  cout << "The default data warehouse directory"
       << " in HDFS will be set to /usr/hive/warehouse"
       << endl;
  cout << "Change the default data warehouse directory? (Y or N) ";

  while (1)
  {
    cin >> type;
    switch (type)
    {
      case 'Y':
        cout << "Enter the absolute path to the data warehouse directory :";
        cin >> data_dir_path;
        break;
      case 'N':
        break;
      default:
        cout << "Enter either Y or N:";
        continue;
    }
    break;
  }

 try
 {
    HDFSSchema sqltohdfs_obj(hdfs_host, hdfs_port, hdfs_user, data_dir_path);

    cout << "The data warehouse directory is set as "
         << sqltohdfs_obj.get_data_warehouse_dir()
         << endl;
    sqltohdfs_obj.hdfs_field_delim= field_delim;
    sqltohdfs_obj.hdfs_row_delim= row_delim;
    /*
      Attach custom event content handlers
    */
    Incident_handler incident_hndlr;
    Table_index table_event_hdlr;
    Applier replay_hndlr(&table_event_hdlr, &sqltohdfs_obj);

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
      int res= binlog.wait_for_next_event(&event);
      if (res != ERR_OK)
        break;

      delete event;
    } // end loop
  }
  catch(std::runtime_error& e)
  {
    cout <<  e.what() << endl;
  }
  return (EXIT_SUCCESS);
}
