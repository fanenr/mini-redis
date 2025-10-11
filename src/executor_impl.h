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
  resp::data exec_expire ();
  resp::data exec_pexpire ();

private:
  typedef system_clock clock_type;
  typedef resp::data (executor_impl::*exec_fn) ();

  config &config_;
  storage::unordered_map<std::string, storage::data> db_;
  storage::unordered_map<std::string, clock_type::time_point> ttl_;

  std::vector<std::reference_wrapper<std::string>> args_;
  const storage::unordered_map<string_view, exec_fn> cmds_;

private:
  typedef decltype (db_)::iterator db_iterator;

  db_iterator db_find (const std::string &key);
  void db_erase (db_iterator it);
}; // class executor::impl

} // namespace mini_redis

#endif // EXECUTOR_IMPL_H
