#ifndef PROCESSOR_H
#define PROCESSOR_H

#include "config.h"
#include "db_storage.h"

namespace mini_redis
{

class processor
{
public:
  explicit processor (config &cfg);

  resp::data execute (resp::data resp);

private:
  // Connection commands
  resp::data exec_ping ();

  // Server commands
  resp::data exec_save ();
  resp::data exec_load ();

  // String commands
  resp::data exec_set ();
  resp::data exec_get ();
  resp::data exec_incr ();
  resp::data exec_incrby ();
  resp::data exec_decr ();
  resp::data exec_decrby ();
  template <template <class> class Op>
  resp::data calc_impl (string_view cmd, bool with_rhs);

  // Generic commands
  resp::data exec_del ();
  resp::data exec_expire ();
  resp::data exec_pexpire ();
  resp::data exec_expireat ();
  resp::data exec_pexpireat ();
  template <class Duration, bool At>
  resp::data expire_impl (string_view cmd);
  resp::data exec_ttl ();
  resp::data exec_pttl ();
  template <class Duration>
  resp::data ttl_impl (string_view cmd);

  // List commands
  resp::data exec_llen ();
  resp::data exec_lindex ();
  resp::data exec_lrange ();

  resp::data exec_lset ();
  resp::data exec_lrem ();
  resp::data exec_linsert ();

  resp::data exec_lpush ();
  resp::data exec_rpush ();
  resp::data exec_lpop ();
  resp::data exec_rpop ();

private:
  config &config_;
  db::storage storage_;
  std::vector<std::string> args_;
}; // class processor

} // namespace mini_redis

#endif // PROCESSOR_H
