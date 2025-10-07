#ifndef DETAIL_PREDEF_H
#define DETAIL_PREDEF_H

#include "../config.h"

#include <boost/asio.hpp>
#include <boost/assert.hpp>
#include <boost/core/make_span.hpp>
#include <boost/core/span.hpp>
#include <boost/optional.hpp>
#include <boost/smart_ptr/make_unique.hpp>
#include <boost/system.hpp>
#include <boost/throw_exception.hpp>
#include <boost/utility/string_view.hpp>
#include <boost/variant2.hpp>

namespace mini_redis
{
namespace detail
{

namespace asio = boost::asio;

using asio::ip::tcp;
using boost::optional;
using boost::span;
using boost::string_view;
using boost::system::error_code;
using boost::variant2::variant;

using boost::make_span;
using boost::make_unique;

} // namespace detail
} // namespace mini_redis

#endif // DETAIL_PREDEF_H
