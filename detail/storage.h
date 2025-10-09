#ifndef DETAIL_STORAGE_H
#define DETAIL_STORAGE_H

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
namespace detail
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

  resp::data
  to_resp () const
  {
    switch (index ())
      {
      case raw_index:
	{
	  const auto &str = get<raw> ();
	  return resp::data{ resp::bulk_string{ str } };
	}
	break;

      case integer_index:
	{
	  const auto &num = get<integer> ();
	  return resp::data{ resp::integer{ num } };
	}
	break;

      case list_index:
	{
	  const auto &lst = get<list> ();

	  std::vector<resp::data> arr;
	  arr.reserve (lst.size ());
	  for (const auto &i : lst)
	    arr.push_back (resp::data{ resp::bulk_string{ i } });

	  return resp::data{ resp::array{ std::move (arr) } };
	}
	break;

      case set_index:
	{
	  const auto &st = get<set> ();

	  std::vector<resp::data> arr;
	  arr.reserve (st.size ());
	  for (const auto &i : st)
	    arr.push_back (resp::data{ resp::bulk_string{ i } });

	  return resp::data{ resp::array{ std::move (arr) } };
	}
	break;

      case hashtable_index:
	{
	  const auto &ht = get<hashtable> ();

	  std::vector<resp::data> arr;
	  arr.reserve (ht.size () * 2);
	  for (const auto &p : ht)
	    {
	      arr.push_back (resp::data{ resp::bulk_string{ p.first } });
	      arr.push_back (resp::data{ resp::bulk_string{ p.second } });
	    }

	  return resp::data{ resp::array{ std::move (arr) } };
	}
	break;

      default:
	BOOST_THROW_EXCEPTION (std::logic_error ("bad data"));
      }
  }
}; // class data

} // namespace storage
} // namespace detail
} // namespace mini_redis

#endif // DETAIL_STORAGE_H
