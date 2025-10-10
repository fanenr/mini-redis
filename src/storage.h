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

typedef value_wrapper<std::string, 0> raw;
typedef value_wrapper<std::int64_t, 1> integer;
typedef value_wrapper<std::deque<std::string>, 2> list;
typedef value_wrapper<unordered_set<std::string>, 3> set;
typedef value_wrapper<unordered_map<std::string, std::string>, 4> hashtable;

typedef variant_wrapper<raw, integer, list, set, hashtable> data_base;

struct data : data_base
{
  typedef data_base base_type;
  using base_type::base_type;

  resp::data to_resp () const;
}; // class data

} // namespace storage
} // namespace mini_redis

#endif // STORAGE_H
