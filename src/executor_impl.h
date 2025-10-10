#ifndef EXECUTOR_IMPL_H
#define EXECUTOR_IMPL_H

#include "config.h"
#include "storage.h"

#include <functional>

namespace mini_redis
{

class executor_impl
{
public:
  explicit executor_impl (config &cfg);

  resp::data execute (resp::data resp);

private:
  resp::data exec (string_view cmd);
  resp::data exec_set ();
  resp::data exec_get ();
  resp::data exec_del ();
  resp::data exec_ping ();

private:
  typedef resp::data (executor_impl::*exec_type) ();

  config &config_;
  storage::unordered_map<std::string, storage::data> db_;

  std::vector<std::reference_wrapper<std::string>> args_;
  const storage::unordered_map<string_view, exec_type> cmds_;
}; // class executor::impl

} // namespace mini_redis

#endif // EXECUTOR_IMPL_H
