#ifndef DETAIL_IMPL_EXECUTOR_H
#define DETAIL_IMPL_EXECUTOR_H

#include "../executor.h"

#include <boost/unordered/unordered_flat_map.hpp>
#include <functional>

namespace mini_redis
{
namespace detail
{

class executor::impl
{
public:
  explicit impl (config &cfg) : config_ (cfg) {}

  resp::data
  execute (resp::data resp)
  {
    auto *p_arr = resp.get_if<resp::array> ();
    if (!p_arr || !p_arr->has_value ())
      return error ("bad command: invalid array");

    auto &arr = p_arr->value ();
    auto validator{ [] (const resp::data &resp) {
      auto p = resp.get_if<resp::bulk_string> ();
      return p && p->has_value ();
    } };
    if (arr.empty () || !std::all_of (arr.begin (), arr.end (), validator))
      return error ("bad command: invalid command");

    std::string &cmd = arr.front ().get<resp::bulk_string> ().value ();
    std::vector<string_view> args (arr.size () - 1);

    auto transformer{ [] (const resp::data &resp) {
      return string_view{ resp.get<resp::bulk_string> ().value () };
    } };
    std::transform (arr.begin () + 1, arr.end (), args.begin (), transformer);

    return exec (cmd, args);
  }

private:
  resp::data
  error (string_view msg) const
  {
    return resp::data{ resp::simple_error{ msg.to_string () } };
  }

  resp::data
  success (string_view msg) const
  {
    return resp::data{ resp::simple_string{ msg.to_string () } };
  }

  resp::data
  null_array () const
  {
    return resp::data{ resp::array{ boost::none } };
  }

  resp::data
  null_string () const
  {
    return resp::data{ resp::bulk_string{ boost::none } };
  }

  resp::data
  exec (string_view cmd, span<string_view> args)
  {
    if (cmd == "SET")
      return exec_set (args);
    else if (cmd == "GET")
      return exec_get (args);

    return error ("bad command: unknown command");
  }

  resp::data
  exec_set (span<string_view> args)
  {
    if (args.size () != 2)
      return error ("bad command: argument count mismatch");

    return success ("OK");
  }

  resp::data
  exec_get (span<string_view> args)
  {
    if (args.size () != 1)
      return error ("bad command: argument count mismatch");

    return null_string ();
  }

private:
  config &config_;
}; // class executor

} // namespace detail
} // namespace mini_redis

namespace mini_redis
{
namespace detail
{

executor::executor (asio::any_io_executor ex, config &cfg)
    : strand_ (ex), impl_ (new impl (cfg))
{
}

executor::~executor () { delete impl_; }

template <class Task>
void
executor::post (Task task)
{
  asio::post (strand_, std::move (task));
}

template <class Task>
void
executor::dispatch (Task task)
{
  asio::dispatch (strand_, std::move (task));
}

template <class Callback>
void
executor::post (resp::data cmd, Callback cb)
{
  auto task{ [this] (resp::data cmd, Callback cb) {
    auto result = this->impl_->execute (std::move (cmd));
    cb (std::move (result));
  } };
  asio::post (strand_, std::bind (task, std::move (cmd), std::move (cb)));
}

template <class Callback>
void
executor::dispatch (resp::data cmd, Callback cb)
{
  auto task{ [this] (resp::data cmd, Callback cb) {
    auto result = this->impl_->execute (std::move (cmd));
    cb (std::move (result));
  } };
  asio::dispatch (strand_, std::bind (task, std::move (cmd), std::move (cb)));
}

} // namespace detail
} // namespace mini_redis

#endif // DETAIL_IMPL_EXECUTOR_H
