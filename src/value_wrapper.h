#ifndef VALUE_WRAPPER_H
#define VALUE_WRAPPER_H

#include "predef.h"

namespace mini_redis
{

template <class T, int Tag>
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
}; // class value_wrapper

template <class T>
struct is_value_wrapper : std::false_type
{
};

template <class T, int Tag>
struct is_value_wrapper<value_wrapper<T, Tag>> : std::true_type
{
};

} // namespace mini_redis

#endif // VALUE_WRAPPER_H
