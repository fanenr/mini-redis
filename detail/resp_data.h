#ifndef DETAIL_RESP_VALUE_H
#define DETAIL_RESP_VALUE_H

#include "predef.h"

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

struct data;

template <class T, int Tag = 0>
struct value_wrapper
{
  typedef T value_type;
  value_type value;

  value_wrapper (const value_type &v) : value{ v } {}
  value_wrapper (value_type &&v) : value{ std::move (v) } {}

  value_type &
  operator* () noexcept
  {
    return value;
  }

  const value_type &
  operator* () const noexcept
  {
    return value;
  }

  value_type *
  operator->() noexcept
  {
    return &value;
  }

  const value_type *
  operator->() const noexcept
  {
    return &value;
  }

  value_type &
  operator() () noexcept
  {
    return value;
  }

  const value_type &
  operator() () const noexcept
  {
    return value;
  }

  friend bool
  operator== (const value_wrapper &lhs, const value_wrapper &rhs)
  {
    return lhs.value == rhs.value;
  }

  friend bool
  operator!= (const value_wrapper &lhs, const value_wrapper &rhs)
  {
    return !(lhs == rhs);
  }
};

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

typedef value_wrapper<std::string, simple_string_index> simple_string;
typedef value_wrapper<std::string, simple_error_index> simple_error;
typedef value_wrapper<optional<std::string>, bulk_string_index> bulk_string;
typedef value_wrapper<std::int64_t, integer_index> integer;
typedef value_wrapper<optional<std::vector<data>>, array_index> array;

struct data
    : value_wrapper<
	  variant<simple_string, simple_error, bulk_string, integer, array>>
{
  typedef value_wrapper base_type;

public:
  using base_type::base_type;

  std::size_t
  index () const noexcept
  {
    return value.index ();
  }

  std::string
  encode () const
  {
    std::ostringstream oss;
    encode_impl (oss, *this);
    return oss.str ();
  }

  template <std::size_t I>
  bool
  is () const noexcept
  {
    return index () == I;
  }

  template <class U>
  bool
  is () const noexcept
  {
    return boost::variant2::holds_alternative<U> (value);
  }

  template <std::size_t I>
  auto
  get () -> decltype (boost::variant2::get<I> (value).value) &
  {
    return boost::variant2::get<I> (value).value;
  }

  template <std::size_t I>
  auto
  get () const -> decltype (boost::variant2::get<I> (value).value) const &
  {
    return boost::variant2::get<I> (value).value;
  }

  template <class U>
  auto
  get () -> decltype (boost::variant2::get<U> (value).value) &
  {
    return boost::variant2::get<U> (value).value;
  }

  template <class U>
  auto
  get () const -> decltype (boost::variant2::get<U> (value).value) const &
  {
    return boost::variant2::get<U> (value).value;
  }

  template <std::size_t I>
  auto
  get_if () noexcept -> decltype (boost::variant2::get_if<I> (&value)->value) *
  {
    auto p = boost::variant2::get_if<I> (&value);
    return p ? &p->value : nullptr;
  }

  template <std::size_t I>
  auto
  get_if () const noexcept
      -> decltype (boost::variant2::get_if<I> (&value)->value) const *
  {
    auto p = boost::variant2::get_if<I> (&value);
    return p ? &p->value : nullptr;
  }

  template <class U>
  auto
  get_if () noexcept -> decltype (boost::variant2::get_if<U> (&value)->value) *
  {
    auto p = boost::variant2::get_if<U> (&value);
    return p ? &p->value : nullptr;
  }

  template <class U>
  auto
  get_if () const noexcept
      -> decltype (boost::variant2::get_if<U> (&value)->value) const *
  {
    auto p = boost::variant2::get_if<U> (&value);
    return p ? &p->value : nullptr;
  }

private:
  void
  encode_impl (std::ostringstream &oss, const data &resp) const
  {
    switch (resp.index ())
      {
      case simple_string_index:
	{
	  const auto &ss = resp.get<simple_error> ();
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
	      auto size = arr->size ();
	      oss << size << "\r\n";
	      for (std::size_t i = 0; i < size; i++)
		encode_impl (oss, arr.value ()[i]);
	    }
	  else
	    oss << -1 << "\r\n";
	}
	break;
      }
  }
}; // class data

} // namespace resp
} // namespace detail
} // namespace mini_redis

#endif // DETAIL_RESP_VALUE_H
