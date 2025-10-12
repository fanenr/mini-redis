#ifndef MANAGER_H
#define MANAGER_H

#include "config.h"
#include "manager_impl.h"
#include "predef.h"
#include "resp_data.h"

namespace mini_redis
{

class manager
{
public:
  manager (asio::any_io_executor ex, config &cfg)
      : strand_ (ex), impl_ (new manager_impl (cfg))
  {
  }

  ~manager () { delete impl_; }

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
  manager_impl *impl_;
}; // class executor

} // namespace mini_redis

#endif // MANAGER_H
