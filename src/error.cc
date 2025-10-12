#include "error.h"

#include <string>

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

const boost::system::error_category &
get_basic_category ()
{
  static basic_category instance;
  return instance;
}

boost::system::error_code
make_error_code (basic_errors e)
{
  return boost::system::error_code (static_cast<int> (e),
				    get_basic_category ());
}

} // namespace error
} // namespace mini_redis
