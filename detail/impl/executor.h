#ifndef DETAIL_IMPL_EXECUTOR_H
#define DETAIL_IMPL_EXECUTOR_H

#include "../executor.h"
#include "../storage.h"

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
      return resp::error ("bad command: invalid array");

    auto &arr = p_arr->value ();
    auto validator{ [] (const resp::data &resp) {
      auto p = resp.get_if<resp::bulk_string> ();
      return p && p->has_value ();
    } };
    if (arr.empty () || !std::all_of (arr.begin (), arr.end (), validator))
      return resp::error ("bad command: invalid command");

    std::string &cmd = arr.front ().get<resp::bulk_string> ().value ();
    std::vector<string_view> args;
    args.reserve (arr.size () - 1);
    for (auto it = arr.begin () + 1; it != arr.end (); it++)
      args.push_back (it->get<resp::bulk_string> ().value ());

    return exec (cmd, args);
  }

private:
  resp::data
  exec (string_view cmd, span<string_view> args)
  {
    if (cmd == "SET")
      return exec_set (args);
    else if (cmd == "GET")
      return exec_get (args);
    return resp::error ("bad command: unknown command");
  }

  resp::data
  exec_set (span<string_view> args)
  {
    if (args.size () != 2)
      return resp::error ("bad command: argument count mismatch");

    auto key = args[0].to_string ();
    auto value = storage::data{ storage::raw{ args[1].to_string () } };
    auto pair = db_.insert_or_assign (std::move (key), std::move (value));

    if (pair.second)
      return resp::success ("OK");
    else
      return resp::error ("FAILED");
  }

  resp::data
  exec_get (span<string_view> args)
  {
    if (args.size () != 1)
      return resp::error ("bad command: argument count mismatch");

    auto key = args[0].to_string ();
    auto it = db_.find (key);

    if (it != db_.end ())
      return it->second.to_resp ();
    else
      return resp::null_string ();
  }

private:
  config &config_;
  storage::unordered_map<std::string, storage::data> db_;
}; // class executor

} // namespace detail
} // namespace mini_redis

namespace mini_redis
{
namespace detail
{

inline executor::executor (asio::any_io_executor ex, config &cfg)
    : strand_ (ex), impl_ (new impl (cfg))
{
}

inline executor::~executor () { delete impl_; }

template <class Task>
inline void
executor::post (Task task)
{
  asio::post (strand_, std::move (task));
}

template <class Task>
inline void
executor::dispatch (Task task)
{
  asio::dispatch (strand_, std::move (task));
}

template <class Callback>
inline void
executor::post (resp::data cmd, Callback cb)
{
  auto task{ [this] (resp::data cmd, Callback cb) {
    auto result = this->impl_->execute (std::move (cmd));
    cb (std::move (result));
  } };
  asio::post (strand_, std::bind (task, std::move (cmd), std::move (cb)));
}

template <class Callback>
inline void
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
