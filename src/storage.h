#ifndef STORAGE_H
#define STORAGE_H

#include "predef.h"
#include "resp_data.h"
#include "value_wrapper.h"
#include "variant_wrapper.h"

#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>
#include <cstdint>
#include <deque>
#include <string>

namespace mini_redis
{
namespace storage
{

template <class Value>
using unordered_set = boost::unordered_flat_set<Value>;

template <class Key, class Value>
using unordered_map = boost::unordered_flat_map<Key, Value>;

enum : std::size_t
{
  raw_index = 0,
  integer_index = 1,
  list_index = 2,
  set_index = 3,
  hashtable_index = 4,
};

typedef value_wrapper<std::string, raw_index> raw;
typedef value_wrapper<std::int64_t, integer_index> integer;
typedef value_wrapper<std::deque<std::string>, list_index> list;
typedef value_wrapper<unordered_set<std::string>, set_index> set;
typedef value_wrapper<unordered_map<std::string, std::string>, hashtable_index>
    hashtable;

struct data : variant_wrapper<raw, integer, list, set, hashtable>
{
  typedef variant_wrapper base_type;
  using base_type::base_type;

  resp::data to_resp () const;
}; // class data

} // namespace storage
} // namespace mini_redis

#endif // STORAGE_H
