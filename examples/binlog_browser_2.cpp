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

#include "binlog_api.h"
#include <getopt.h>
#include <iostream>
#include <iomanip>
#include <stdlib.h>
#include <errno.h>
#define MAX_BINLOG_SIZE 1073741824
#define MAX_BINLOG_POSITION MAX_BINLOG_SIZE/4


/*
  @file binlog_browser_2

  This program code opens a binary log and prints out what events are found.
  It allows user to specify command line options including start and stop
  positions, offset from the beginning of the binlog file, as well as database
  names. With --database option, event type and positions for statements
  executed on that particular database will only be printed. Other options
  include --event_type, which will print out only that event which is
  specified by the user.
*/

using namespace std;
using mysql::Binary_log;
using mysql::system::create_transport;
using mysql::system::get_event_type_str;

string opt_database;
string opt_event_type;
long int opt_start_pos= 4;
long int opt_offset= 0;
int opt_event_flag= 0;
//The flag is set if user specifies --event_type option in the argument
int opt_db_flag= 0;
//The flag is set if user specifies --database option in the argument
long int opt_stop_pos= MAX_BINLOG_POSITION;
//Default value for opt_stop_pos is the maximum allowed binlog size value.

string event_types[]= { "Append_block",
                        "Begin_load_query",
                        "Create_file",
                        "Delete_file",
                        "Delete_rows",
                        "Delete_rows_event_old",
                        "Exec_load",
                        "Execute_load_query",
                        "Format_desc",
                        "Incident",
                        "Intvar",
                        "Load",
                        "New_load",
                        "Query",
                        "RAND",
                        "Rotate",
                        "Slave",
                        "Start_v3",
                        "Stop",
                        "Table_map",
                        "Unknown",
                        "Update_rows",
                        "Update_rows_event_old",
                        "User defined",
                        "User var",
                        "Write_rows",
                        "Write_rows_event_old",
                        "Xid"};

int number_of_events= sizeof(event_types)/sizeof(event_types[0]);

static void usage(const struct option *options);

/**
  This function is called internally from check_event_db function.
  It compares the database names obatained from the user
  and that got from the event in the binlog file.

  @param log_dbname  It is the name got from the event in the binlog file.

  @return status
    @retval true  The event on the database log_dbname are to be skipped.
    @retval false The events are to be printed.
*/
bool shall_skip_database(const string& log_dbname)
{
  if (!log_dbname.empty() && !opt_database.empty())
    if (opt_database == log_dbname)
      return false;
  return true;
}

void print_accepatble_event_type()
{
  cout << "Allowed values for (--event_type=arg)" << endl;
  for (int i= 0; i < number_of_events; i++)
  {
    cout << event_types[i] << endl;
  }
}

bool check_opt_event_type(string optarg)
{
  return binary_search(event_types, event_types+number_of_events, optarg);
}

bool check_event_type(Binary_log_event **event)
{
  string log_event_type= get_event_type_str((*event)->get_event_type());

  if (!opt_event_type.empty() && !log_event_type.empty())
    if (opt_event_type == log_event_type)
      return false;
  return true;
}

/**
  The function takes in events from the binlog file, gets
  the database on which the event occurred, and returns status
  whether the database is same as that  given by the user.

  @param event   events read from the binlog file via the wait_for_next_event
                 in the main function

  @return Status
    @retval true    Database name does not match the name given by the user and
                    the event must  be skipped
    @retval false   The database for the event is the same as that specified
                    by the user.
*/

static bool check_event_db(Binary_log_event **event)
{
  static int m_table_id;
  switch ((*event)->get_event_type())
  {
  case QUERY_EVENT:
    {
      Query_event *query_event= dynamic_cast<Query_event*>(*event);
      if (query_event == 0)
        return false;

      return shall_skip_database(query_event->db_name);
    }
  case TABLE_MAP_EVENT:
    {
      Table_map_event *table_map_event= dynamic_cast<Table_map_event*>(*event);
      if (table_map_event == 0)
        return false;

      if (shall_skip_database(table_map_event->db_name))
      {
        m_table_id= table_map_event->table_id;
        /*
          m_table_id takes a zero value in case the event must not be skipped.
          Else, the value is the table-id of the table map event which is to be
          skipped. It is used while checking databases for the row events.
        */
        return true;
      }
      return false;
    }
  case PRE_GA_WRITE_ROWS_EVENT:
  case PRE_GA_UPDATE_ROWS_EVENT:
  case PRE_GA_DELETE_ROWS_EVENT:
  case WRITE_ROWS_EVENT:
  case UPDATE_ROWS_EVENT:
  case DELETE_ROWS_EVENT:
    {
      Row_event *row_event= dynamic_cast<Row_event*>(*event);
      if (row_event == 0)
        return false;

      if (row_event->table_id == m_table_id)
        return true;
      return false;
    }
  default:
    return false;
  //TODO Add cases for CREATE_FILE_EVENT and EXECUTE_LOAD_QUERY_EVENT.
  /*
     Currently, the API has no classes for these events,
     and they go into the default case. Classes are to be added to the
     repository. Reference file: binlog_driver.h.
  */
  }//end switch
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
  else
  {
    return true;
  }
}


static void parse_args(int *argc, char **argv)
{
  static struct option long_options[]=
  {
    //These options take an argument
    {"start_position", required_argument, 0, 'a'},
    {"stop_position", required_argument, 0, 'b'},
    {"database", required_argument, 0, 'd'},
    {"offset", required_argument, 0, 'o'},
    {"event_type", required_argument, 0, 'e'},
    {0, 0, 0, 0}
  };

  while (true)
  {
    //getopt_long stores the option index here
    int option_index= 0;
    int c= getopt_long(*argc, argv, "a:b:d:o:e:", long_options, &option_index);
    //Detect the end of options
    if (c == -1)
      break;
    switch (c)
    {
    case 'a':
      if (!check_conversion(optarg, &opt_start_pos))
      {
        cerr << optarg << "is not an integer" << endl;
        usage(long_options);
        exit(2);
      }
      if (opt_start_pos < 4)
      {
        cerr << "option 'start-position': value " << opt_start_pos
             << " adjusted to 4"
             << endl;
        opt_start_pos= 4;
      }
      break;
    case 'b':
      if (!check_conversion(optarg, &opt_stop_pos))
      {
        cerr << optarg << "is not an integer" << endl;
        usage(long_options);
        exit(2);
      }
      break;
    case 'd':
      {
        opt_database= optarg;
        opt_db_flag= 1;
        break;
      }
    case 'o':
      if (!check_conversion(optarg, &opt_offset) )
      {
        cerr << optarg << "is not an integer" << endl;
        usage(long_options);
        exit(2);
      }
      if (opt_offset < 0)
      {
        cerr << "option 'offset': value " << opt_offset
             << "adjusted to 0"
             << endl;
        opt_offset= 0;
      }
      break;
    case 'e':
      if (!check_opt_event_type(optarg))
      {
        usage(long_options);
        exit(2);
      }
      opt_event_type= optarg;
      opt_event_flag= 1;
      break;
    case '?':
    default:
      //getopt has already printed out the error
      usage(long_options);
      exit(2);
    }//end of switch
  }//end of while loop

  if (optind == *argc)
  {
    usage(long_options);
    exit(2);
  }
  else
  {
    *argc= optind;
  }
}//endparse_args

static void usage(const struct option *options)
{
  const struct option *optp;
  cerr << "Usage: ./binlog_browser [options]  log-files\n";
  for (optp= options; optp->name; optp++)
  {
    cout << "-" << (char)optp->val
         << (strlen(optp->name) ? ", " : "  ")
         << "--" << optp->name;
    if (optp->has_arg  == 1)
      cout << "=arg";
    cout << endl;
  }
  print_accepatble_event_type();
}

int main(int argc, char** argv)
{
  string uri;
  int number_of_args= argc;

  parse_args(&argc, argv);

  while (argc != number_of_args)
  {
    uri= argv[argc++];
    Binary_log binlog(create_transport(uri.c_str()));
    if (binlog.connect() != ERR_OK)
    {
      cerr << "Unable to setup connection" << endl;
      exit(2);
    }

    binlog.set_position(opt_start_pos);
    cout << setw(17) << left
         << "Start Position"
         << setw(15) << left
         << "End Position"
         << setw(15) << left
         << "Event Length"
         << "Event Type"
         << endl;

    while (true)
    {
      Binary_log_event *event;
      const int result= binlog.wait_for_next_event(&event);
      long int event_start_pos;
      string event_type;

      if (result == ERR_EOF)
        break;

      if (event->get_event_type() == mysql::INCIDENT_EVENT ||
         (event->get_event_type() == mysql::ROTATE_EVENT && event->header()->next_position == 0))
      {
          /*
            If the event type is an Incident event or a Rotate event
            which occurs when a server starts on a fresh binlog file,
            the event will not be written in the binlog file.
          */
          event_start_pos= 0;

      }
      else
      {
        event_start_pos= binlog.get_position() -
                       (event->header())->event_length;
      }

      if (event_start_pos >= opt_stop_pos)
        break;

      if ((opt_offset > 0 && opt_offset--) ||
          (opt_db_flag && check_event_db(&event)) ||
          (opt_event_flag && check_event_type(&event)))
      {
        continue;
      }

      cout << setw(17) << left
           << event_start_pos
           << setw(15) << left
           << (event->header())->next_position
           << setw(15) << left
           << (event->header())->event_length
           << get_event_type_str(event->get_event_type())
           << "[" << event->get_event_type() << "]"
           << endl;
    }
  }//end of outer while loop
}
