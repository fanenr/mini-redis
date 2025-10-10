#ifndef DETAIL_RESP_VALUE_H
#define DETAIL_RESP_VALUE_H

#include "predef.h"
#include "value_wrapper.h"
#include "variant_wrapper.h"

#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

namespace mini_redis
{
namespace detail
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

  static data
  null_array ()
  {
    return data{ array{ boost::none } };
  }

  static data
  null_string ()
  {
    return data{ bulk_string{ boost::none } };
  }

  static data
  empty_array ()
  {
    return data{ array{ std::vector<data>{} } };
  }

  static data
  empty_string ()
  {
    return data{ bulk_string{ std::string{} } };
  }

  static data
  error (string_view msg)
  {
    return data{ simple_error{ msg.to_string () } };
  }

  static data
  message (string_view msg)
  {
    return data{ simple_string{ msg.to_string () } };
  }

  std::string
  encode () const
  {
    std::ostringstream oss;
    encode (oss, *this);
    return oss.str ();
  }

  static void
  encode (std::ostringstream &oss, const data &resp)
  {
    switch (resp.index ())
      {
      case simple_string_index:
	{
	  const auto &ss = resp.get<simple_string> ();
	  oss << simple_string_first << ss << "\r\n";
	}
	break;

      case simple_error_index:
	{
	  const auto &se = resp.get<simple_error> ();
	  oss << simple_error_first << se << "\r\n";
	}
	break;

      case bulk_string_index:
	{
	  const auto &bs = resp.get<bulk_string> ();
	  oss << bulk_string_first;
	  if (bs)
	    oss << bs->size () << "\r\n" << *bs << "\r\n";
	  else
	    oss << -1 << "\r\n";
	}
	break;

      case integer_index:
	{
	  const auto &num = resp.get<integer> ();
	  oss << integer_first << num << "\r\n";
	}
	break;

      case array_index:
	{
	  const auto &arr = resp.get<array> ();
	  oss << array_first;
	  if (arr)
	    {
	      const auto &vec = arr.value ();
	      oss << vec.size () << "\r\n";
	      for (const auto &i : vec)
		encode (oss, i);
	    }
	  else
	    oss << -1 << "\r\n";
	}
	break;

      default:
	BOOST_THROW_EXCEPTION (std::logic_error ("bad data"));
      }
  }
}; // class data

} // namespace resp
} // namespace detail
} // namespace mini_redis

#endif // DETAIL_RESP_VALUE_H
