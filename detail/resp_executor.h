#ifndef DETAIL_RESP_EXECUTOR_H
#define DETAIL_RESP_EXECUTOR_H

#include "resp_data.h"

#include <boost/unordered/unordered_flat_map.hpp>

namespace mini_redis
{
namespace detail
{
namespace resp
{

class executor
{
public:
  explicit executor (const config &cfg) : config_ (cfg) {}

  data
  execute (data resp)
  {
    auto *p_arr = resp.get_if<array_index> ();
    if (!p_arr || !p_arr->has_value () || p_arr->value ().empty ())
      return data{ simple_error{
	  std::string{ "bad command: invalid array" } } };

    auto &arr = p_arr->value ();
    auto *p_cmd = arr[0].get_if<bulk_string_index> ();
    if (!p_cmd || !p_cmd->has_value ())
      return data{ simple_error{
	  std::string{ "bad command: invalid command" } } };

    auto args = make_span (arr.data () + 1, arr.size () - 1);
    return execute (p_cmd->value (), args);
  }

  const config &
  get_config () const
  {
    return config_;
  }

private:
  data
  execute (string_view cmd, span<data> args)
  {
  }

private:
  config config_;
  boost::unordered_flat_map<std::string, data> storage_;
}; // class executor

} // namespace resp
} // namespace detail
} // namespace mini_redis

#endif // DETAIL_RESP_EXECUTOR_H
