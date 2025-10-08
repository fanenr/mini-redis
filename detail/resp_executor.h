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
    if (!p_arr || !p_arr->has_value ())
      return data{ simple_error{
	  std::string{ "bad command: invalid array" } } };

    auto &arr = p_arr->value ();
    auto validator{ [] (const data &resp) {
      auto p = resp.get_if<bulk_string_index> ();
      return p && p->has_value ();
    } };
    if (arr.empty () || !std::all_of (arr.begin (), arr.end (), validator))
      return data{ simple_error{
	  std::string{ "bad command: invalid command" } } };

    args_.resize (arr.size () - 1);
    auto &cmd = arr[0].get<bulk_string_index> ().value ();
    auto transformer{ [] (const data &resp) {
      return string_view{ resp.get<bulk_string_index> ().value () };
    } };
    std::transform (arr.begin () + 1, arr.end (), args_.begin (), transformer);

    return execute (cmd, args_);
  }

  const config &
  get_config () const
  {
    return config_;
  }

private:
  data
  execute (string_view cmd, span<string_view> args)
  {
  }

private:
  config config_;
  std::vector<string_view> args_;
  boost::unordered_flat_map<std::string, data> storage_;
}; // class executor

} // namespace resp
} // namespace detail
} // namespace mini_redis

#endif // DETAIL_RESP_EXECUTOR_H
