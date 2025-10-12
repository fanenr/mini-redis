#ifndef PREDEF_H
#define PREDEF_H

#include <boost/asio.hpp>
#include <boost/assert.hpp>
#include <boost/core/make_span.hpp>
#include <boost/core/span.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <boost/smart_ptr/make_unique.hpp>
#include <boost/system.hpp>
#include <boost/throw_exception.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>
#include <boost/utility/string_view.hpp>
#include <boost/variant2.hpp>
#include <chrono>

namespace mini_redis
{

namespace asio = boost::asio;
namespace chrono = std::chrono;

using asio::ip::tcp;

using boost::optional;
using boost::span;
using boost::string_view;
using boost::unordered_flat_map;
using boost::unordered_flat_set;
using boost::system::error_code;
using boost::variant2::variant;

using boost::lexical_cast;
using boost::make_span;
using boost::make_unique;
using boost::conversion::try_lexical_convert;

using chrono::milliseconds;
using chrono::seconds;
using chrono::steady_clock;
using chrono::system_clock;

using chrono::duration_cast;

} // namespace mini_redis

#endif // PREDEF_H
