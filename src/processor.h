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
  resp::data exec_ping ();

  resp::data exec_set ();
  resp::data exec_get ();
  resp::data exec_del ();

  resp::data exec_expire ();
  resp::data exec_pexpire ();
  resp::data exec_expireat ();
  resp::data exec_pexpireat ();
  template <class Duration, bool At>
  resp::data generic_expire ();

  resp::data exec_ttl ();
  resp::data exec_pttl ();
  template <class Duration>
  resp::data generic_ttl ();

  resp::data exec_incr ();
  resp::data exec_incrby ();
  resp::data exec_decr ();
  resp::data exec_decrby ();
  template <template <class T> class Op>
  resp::data generic_calc ();

private:
  config &config_;
  db::storage storage_;
  std::vector<std::reference_wrapper<std::string>> args_;

  typedef resp::data (processor::*exec_fn) ();
  const unordered_flat_map<string_view, exec_fn> exec_map_{
    { "PING", &processor::exec_ping },

    { "SET", &processor::exec_set },
    { "GET", &processor::exec_get },
    { "DEL", &processor::exec_del },

    { "EXPIRE", &processor::exec_expire },
    { "PEXPIRE", &processor::exec_pexpire },
    { "EXPIREAT", &processor::exec_expireat },
    { "PEXPIREAT", &processor::exec_pexpireat },

    { "TTL", &processor::exec_ttl },
    { "PTTL", &processor::exec_pttl },

    { "INCR", &processor::exec_incr },
    { "INCRBY", &processor::exec_incrby },
    { "DECR", &processor::exec_decr },
    { "DECRBY", &processor::exec_decrby },
  };
}; // class processor

} // namespace mini_redis

#endif // PROCESSOR_H
