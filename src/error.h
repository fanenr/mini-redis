#ifndef ERROR_H
#define ERROR_H

#include <boost/system.hpp>

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

#include "error_impl.h"

#endif // ERROR_H
