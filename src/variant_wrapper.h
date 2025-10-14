#ifndef VARIANT_WRAPPER_H
#define VARIANT_WRAPPER_H

#include "predef.h"
#include "value_wrapper.h"

namespace mini_redis
{

template <class... Ts>
struct variant_wrapper : variant<Ts...>
{
  static_assert (
      mp11::mp_all_of<mp11::mp_list<Ts...>, is_value_wrapper>::value,
      "variant_wrapper is only applicable to value_wrapper");

  typedef variant<Ts...> variant_type;
  using variant_type::variant_type;

  template <class U>
  bool
  is () const noexcept
  {
    return boost::variant2::holds_alternative<U> (*this);
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

  template <class U>
  static constexpr std::size_t
  index_of ()
  {
    using namespace boost::mp11;
    typedef mp_list<Ts...> L;
    static_assert (mp_find<L, U>::value < mp_size<L>::value,
		   "U is not in the type list");
    return mp_find<L, U>::value;
  };
}; // class variant_wrapper

} // namespace mini_redis

#endif // VARIANT_WRAPPER_H
