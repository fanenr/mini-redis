#ifndef DETAIL_CONTEXT_H
#define DETAIL_CONTEXT_H

#include "predef.h"
#include "resp_executor.h"

#include <functional>

namespace mini_redis
{
namespace detail
{

class executor
{
public:
  executor (asio::any_io_executor ex, const config &cfg)
      : strand_ (ex), executor_ (cfg)
  {
  }

  template <class Task>
  void
  post (Task task)
  {
    asio::post (strand_, std::move (task));
  }

  template <class Task>
  void
  dispatch (Task task)
  {
    asio::dispatch (strand_, std::move (task));
  }

  template <class Callback>
  void
  post (resp::data cmd, Callback cb)
  {
    auto task{ [this] (resp::data cmd, Callback cb) {
      auto result = this->executor_.execute (std::move (cmd));
      cb (std::move (result));
    } };
    asio::post (strand_, std::bind (task, std::move (cmd), std::move (cb)));
  }

  template <class Callback>
  void
  dispatch (resp::data cmd, Callback cb)
  {
    auto task{ [this] (resp::data cmd, Callback cb) {
      auto result = this->executor_.execute (std::move (cmd));
      cb (std::move (result));
    } };
    asio::dispatch (strand_,
		    std::bind (task, std::move (cmd), std::move (cb)));
  }

  const config &
  get_config () const
  {
    return executor_.get_config ();
  }

private:
  asio::strand<asio::any_io_executor> strand_;
  resp::executor executor_;
}; // class executor

} // namespace detail
} // namespace mini_redis

#endif // DETAIL_EXECUTOR_H
