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

enum : char
{
  simple_string_first = '+',
  simple_error_first = '-',
  bulk_string_first = '$',
  integer_first = ':',
  array_first = '*',
};

struct data;

typedef value_wrapper<std::int64_t, 0> integer;
typedef value_wrapper<std::string, 1> simple_error;
typedef value_wrapper<std::string, 2> simple_string;
typedef value_wrapper<optional<std::string>, 3> bulk_string;
typedef value_wrapper<optional<std::vector<data>>, 4> array;

typedef variant_wrapper<integer, simple_error, simple_string, bulk_string,
			array>
    data_base;

struct data : data_base
{
  typedef data_base base_type;
  using base_type::base_type;

  std::string encode () const;
  static void encode (std::ostringstream &oss, const data &resp);
}; // class data

} // namespace resp
} // namespace mini_redis

#endif // RESP_DATA_H
