#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "config.h"
#include "executor_impl.h"
#include "predef.h"
#include "resp_data.h"

namespace mini_redis
{

class executor
{
public:
  executor (asio::any_io_executor ex, config &cfg)
      : strand_ (ex), impl_ (new executor_impl (cfg))
  {
  }

  ~executor () { delete impl_; }

  template <class Task>
  inline void
  post (Task task)
  {
    asio::post (strand_, std::move (task));
  }

  template <class Task>
  inline void
  dispatch (Task task)
  {
    asio::dispatch (strand_, std::move (task));
  }

  template <class Callback>
  inline void
  post (resp::data cmd, Callback cb)
  {
    auto task{ [this] (resp::data cmd, Callback cb) {
      auto result = this->impl_->execute (std::move (cmd));
      cb (std::move (result));
    } };
    asio::post (strand_, std::bind (task, std::move (cmd), std::move (cb)));
  }

  template <class Callback>
  inline void
  dispatch (resp::data cmd, Callback cb)
  {
    auto task{ [this] (resp::data cmd, Callback cb) {
      auto result = this->impl_->execute (std::move (cmd));
      cb (std::move (result));
    } };
    asio::dispatch (strand_,
		    std::bind (task, std::move (cmd), std::move (cb)));
  }

private:
  asio::strand<asio::any_io_executor> strand_;
  executor_impl *impl_;
}; // class executor

} // namespace mini_redis

#endif // EXECUTOR_H
