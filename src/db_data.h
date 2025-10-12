#ifndef DB_DATA_H
#define DB_DATA_H

#include "predef.h"
#include "resp_data.h"
#include "value_wrapper.h"
#include "variant_wrapper.h"

#include <cstdint>
#include <deque>
#include <string>

namespace mini_redis
{
namespace db
{

typedef value_wrapper<std::string, 0> string;
typedef value_wrapper<std::int64_t, 1> integer;
typedef value_wrapper<std::deque<std::string>, 2> list;
typedef value_wrapper<unordered_flat_set<std::string>, 3> set;
typedef value_wrapper<unordered_flat_map<std::string, std::string>, 4>
    hashtable;

typedef variant_wrapper<string, integer, list, set, hashtable> data_base;

struct data : data_base
{
  typedef data_base base_type;
  using base_type::base_type;

  resp::data to_resp () const;
}; // class data

} // namespace db
} // namespace mini_redis

#endif // DB_DATA_H
