#ifndef DETAIL_EXECUTOR_H
#define DETAIL_EXECUTOR_H

#include "resp_data.h"

namespace mini_redis
{
namespace detail
{

class executor
{
private:
  class impl;

public:
  executor (asio::any_io_executor ex, config &cfg);
  ~executor ();

  template <class Task>
  void post (Task task);

  template <class Task>
  void dispatch (Task task);

  template <class Callback>
  void post (resp::data cmd, Callback cb);

  template <class Callback>
  void dispatch (resp::data cmd, Callback cb);

private:
  asio::strand<asio::any_io_executor> strand_;
  impl *impl_;
}; // class executor

} // namespace detail
} // namespace mini_redis

#include "impl/executor.h"

#endif // DETAIL_EXECUTOR_H
