#include "table_update.h"
#include "table_insert.h"
#include "table_delete.h"

void table_update(std::string table_name, MySQL::Row_of_fields &old_fields, MySQL::Row_of_fields &new_fields)
{
  /*
    Find previous entry and delete it.
  */
  table_delete(table_name, old_fields);

  /*
    Insert new entry.
  */
  table_insert(table_name, new_fields);

}

