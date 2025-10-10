#ifndef VARIANT_WRAPPER_H
#define VARIANT_WRAPPER_H

#include "predef.h"
#include "value_wrapper.h"

namespace mini_redis
{

template <class... Ts>
struct variant_wrapper : variant<Ts...>
{
  typedef variant<Ts...> variant_type;
  using variant_type::variant_type;

  template <std::size_t I>
  bool
  is () const noexcept
  {
    return variant_type::index () == I;
  }

  template <class U>
  bool
  is () const noexcept
  {
    return boost::variant2::holds_alternative<U> (*this);
  }

  template <std::size_t I>
  auto
  get () -> decltype (boost::variant2::get<I> (*this).value) &
  {
    return boost::variant2::get<I> (*this).value;
  }

  template <std::size_t I>
  auto
  get () const -> decltype (boost::variant2::get<I> (*this).value) const &
  {
    return boost::variant2::get<I> (*this).value;
  }

  template <class U>
  auto
  get () -> decltype (boost::variant2::get<U> (*this).value) &
  {
    return boost::variant2::get<U> (*this).value;
  }

  template <class U>
  auto
  get () const -> decltype (boost::variant2::get<U> (*this).value) const &
  {
    return boost::variant2::get<U> (*this).value;
  }

  template <std::size_t I>
  auto
  get_if () noexcept -> decltype (boost::variant2::get_if<I> (this)->value) *
  {
    auto p = boost::variant2::get_if<I> (this);
    return p ? &p->value : nullptr;
  }

  template <std::size_t I>
  auto
  get_if () const noexcept
      -> decltype (boost::variant2::get_if<I> (this)->value) const *
  {
    auto p = boost::variant2::get_if<I> (this);
    return p ? &p->value : nullptr;
  }

  template <class U>
  auto
  get_if () noexcept -> decltype (boost::variant2::get_if<U> (this)->value) *
  {
    auto p = boost::variant2::get_if<U> (this);
    return p ? &p->value : nullptr;
  }

  template <class U>
  auto
  get_if () const noexcept
      -> decltype (boost::variant2::get_if<U> (this)->value) const *
  {
    auto p = boost::variant2::get_if<U> (this);
    return p ? &p->value : nullptr;
  }
}; // class variant_wrapper

} // namespace mini_redis

#endif // VARIANT_WRAPPER_H
