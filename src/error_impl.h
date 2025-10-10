#ifndef ERROR_IMPL_H
#define ERROR_IMPL_H

#include "error.h"

#include <string>
#include <system_error>

namespace mini_redis
{
namespace error
{

class basic_category : public boost::system::error_category
{
public:
  const char *
  name () const noexcept override
  {
    return "mini_redis.basic";
  }

  std::string
  message (int ev) const override
  {
    switch (static_cast<basic_errors> (ev))
      {
      case basic_errors::none:
	return "none";
      default:
	return "unknown";
      }
  }
};

inline const boost::system::error_category &
get_basic_category ()
{
  static const basic_category instance;
  return instance;
}

inline boost::system::error_code
make_error_code (basic_errors e)
{
  return boost::system::error_code (static_cast<int> (e),
				    get_basic_category ());
}

} // namespace error
} // namespace mini_redis

namespace boost
{
namespace system
{

template <>
struct is_error_code_enum<mini_redis::error::basic_errors> : std::true_type
{
};

} // namespace system
} // namespace boost

namespace std
{

template <>
struct is_error_code_enum<mini_redis::error::basic_errors> : std::true_type
{
};

} // namespace std

#endif // ERROR_IMPL_H
