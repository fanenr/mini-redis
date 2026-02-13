#ifndef ERROR_H
#define ERROR_H

#include "pch.h"

namespace mini_redis
{
namespace error
{

enum basic_errors
{
  none = 0,
};

class basic_category;

const boost::system::error_category &get_basic_category ();
boost::system::error_code make_error_code (basic_errors e);

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

#endif // ERROR_H
