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
private:
  typedef resp::data (impl::*exec_type) ();

public:
  explicit impl (config &cfg) : config_ (cfg) {}

  resp::data
  execute (resp::data resp)
  {
    if (!resp.is<resp::array> ())
      return resp::error ("bad command: not an array");

    auto &arr = resp.get<resp::array> ();
    if (!arr.has_value ())
      return resp::error ("bad command: null array");

    auto &vec = arr.value ();
    if (vec.empty ())
      return resp::error ("bad command: empty array");

    auto validator{ [] (const resp::data &resp) {
      auto p = resp.get_if<resp::bulk_string> ();
      return p && p->has_value ();
    } };
    if (!std::all_of (vec.begin (), vec.end (), validator))
      return resp::error ("bad command: invalid arguments");

    auto &cmd = vec.front ().get<resp::bulk_string> ().value ();

    args_.clear ();
    args_.reserve (vec.size () - 1);
    for (auto it = vec.begin () + 1; it != vec.end (); it++)
      args_.push_back (it->get<resp::bulk_string> ().value ());

    return exec (cmd);
  }

private:
  resp::data
  exec (string_view cmd)
  {
    auto it = cmds_.find (cmd);
    if (it != cmds_.end ())
      {
	auto fn = it->second;
	return (this->*fn) ();
      }
    return resp::error ("bad command: unknown command");
  }

  resp::data
  exec_set ()
  {
    if (args_.size () != 2)
      return resp::error ("bad command: argument count mismatch");

    auto &s0 = args_[0].get ();
    auto &s1 = args_[1].get ();
    storage::data data{ storage::raw{ std::move (s1) } };
    auto pair = db_.insert_or_assign (std::move (s0), std::move (data));

    if (pair.second)
      return resp::success ("OK");
    else
      return resp::error ("FAILED");
  }

  resp::data
  exec_get ()
  {
    if (args_.size () != 1)
      return resp::error ("bad command: argument count mismatch");

    auto &s0 = args_[0].get ();
    auto it = db_.find (s0);

    if (it != db_.end ())
      return it->second.to_resp ();
    else
      return resp::null_string ();
  }

private:
  config &config_;
  storage::unordered_map<std::string, storage::data> db_;

  std::vector<std::reference_wrapper<std::string>> args_;
  const storage::unordered_map<string_view, exec_type> cmds_{
    { "SET", &impl::exec_set },
    { "GET", &impl::exec_get },
  };
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
