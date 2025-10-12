#ifndef MANAGER_IMPL_H
#define MANAGER_IMPL_H

#include "config.h"
#include "db_storage.h"

#include <functional>

namespace mini_redis
{

class manager_impl
{
public:
  explicit manager_impl (config &cfg);

  resp::data execute (resp::data resp);

private:
  resp::data exec (string_view cmd);

  resp::data exec_set ();
  resp::data exec_get ();
  resp::data exec_del ();
  resp::data exec_ttl ();
  resp::data exec_pttl ();
  resp::data exec_ping ();
  resp::data exec_expire ();
  resp::data exec_pexpire ();

private:
  config &config_;
  db::storage storage_;
  std::vector<std::reference_wrapper<std::string>> args_;

  typedef resp::data (manager_impl::*exec_fn) ();
  const unordered_flat_map<string_view, exec_fn> exec_map_{
    { "PING", &manager_impl::exec_ping },
    { "SET", &manager_impl::exec_set },
    { "GET", &manager_impl::exec_get },
    { "DEL", &manager_impl::exec_del },
    { "EXPIRE", &manager_impl::exec_expire },
    { "PEXPIRE", &manager_impl::exec_pexpire },
    { "TTL", &manager_impl::exec_ttl },
    { "PTTL", &manager_impl::exec_pttl },
  };
}; // class executor::impl

} // namespace mini_redis

#endif // MANAGER_IMPL_H
