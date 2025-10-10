#ifndef RESP_UTIL_H
#define RESP_UTIL_H

#include "resp_data.h"

namespace mini_redis
{
namespace resp
{

inline data
null_array ()
{
  return data{ array{ boost::none } };
}

inline data
null_string ()
{
  return data{ bulk_string{ boost::none } };
}

inline data
empty_array ()
{
  return data{ array{ std::vector<data>{} } };
}

inline data
empty_string ()
{
  return data{ bulk_string{ std::string{} } };
}

inline data
error (string_view msg)
{
  return data{ simple_error{ msg.to_string () } };
}

inline data
message (string_view msg)
{
  return data{ simple_string{ msg.to_string () } };
}

} // namespace resp
} // namespace mini_redis

#endif // RESP_UTIL_H
