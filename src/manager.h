#ifndef MANAGER_H
#define MANAGER_H

#include "config.h"
#include "predef.h"
#include "processor.h"
#include "resp_data.h"

namespace mini_redis
{

class manager
{
public:
  explicit manager (asio::any_io_executor ex, config &cfg)
      : strand_{ ex }, processor_{ cfg }
  {
  }

  template <class Task>
  inline void
  post (Task task)
  {
    asio::post (strand_, std::bind (std::move (task), &processor_));
  }

  template <class Task>
  inline void
  dispatch (Task task)
  {
    asio::dispatch (strand_, std::bind (std::move (task), &processor_));
  }

private:
  asio::strand<asio::any_io_executor> strand_;
  processor processor_;
}; // class manager

} // namespace mini_redis

#endif // MANAGER_H
