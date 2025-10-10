#ifndef RESP_DATA_H
#define RESP_DATA_H

#include "predef.h"
#include "value_wrapper.h"
#include "variant_wrapper.h"

#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

namespace mini_redis
{
namespace resp
{

enum : std::size_t
{
  simple_string_index = 0,
  simple_error_index = 1,
  bulk_string_index = 2,
  integer_index = 3,
  array_index = 4,
};

enum : char
{
  simple_string_first = '+',
  simple_error_first = '-',
  bulk_string_first = '$',
  integer_first = ':',
  array_first = '*',
};

struct data;

typedef value_wrapper<std::string, simple_string_index> simple_string;
typedef value_wrapper<std::string, simple_error_index> simple_error;
typedef value_wrapper<optional<std::string>, bulk_string_index> bulk_string;
typedef value_wrapper<std::int64_t, integer_index> integer;
typedef value_wrapper<optional<std::vector<data>>, array_index> array;

struct data
    : variant_wrapper<simple_string, simple_error, bulk_string, integer, array>
{
  typedef variant_wrapper base_type;
  using base_type::base_type;

  std::string encode () const;
  static void encode (std::ostringstream &oss, const data &resp);
}; // class data

} // namespace resp
} // namespace mini_redis

#endif // RESP_DATA_H
