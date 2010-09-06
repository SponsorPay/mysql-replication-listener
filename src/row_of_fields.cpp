#include <vector>

#include "row_of_fields.h"
#include <stdexcept>
#include <boost/foreach.hpp>
#include "value.h"

using namespace MySQL;

Row_of_fields& Row_of_fields::operator=(const Row_of_fields &right)
{
  if (size() != right.size())
    throw std::length_error::length_error("Row dimension doesn't match.");
  int i= 0;
  BOOST_FOREACH(Value value, right)
  {
    this->assign(++i, value);
  }
  return *this;
}

Row_of_fields& Row_of_fields::operator=(Row_of_fields &right)
{
  if (size() != right.size())
    throw std::length_error::length_error("Row dimension doesn't match.");
  int i= 0;
  BOOST_FOREACH(Value value, right)
  {
    this->assign(++i, value);
  }
  return *this;
}
