#include "executor_impl.h"

namespace mini_redis
{

static inline resp::data
simple_string (std::string msg)
{
  return resp::data{ resp::simple_string{ std::move (msg) } };
}

static inline resp::data
simple_error (std::string msg)
{
  return resp::data{ resp::simple_error{ std::move (msg) } };
}

static inline resp::data
bulk_string (std::string str)
{
  return resp::data{ resp::bulk_string{ std::move (str) } };
}

static inline resp::data
null_bulk_string ()
{
  return resp::data{ resp::bulk_string{ boost::none } };
}

static inline resp::data
integer (std::int64_t num)
{
  return resp::data{ resp::integer{ num } };
}

} // namespace mini_redis

namespace mini_redis
{

executor_impl::executor_impl (config &cfg)
    : config_ (cfg), cmds_{
	{ "SET", &executor_impl::exec_set },
	{ "GET", &executor_impl::exec_get },
	{ "DEL", &executor_impl::exec_del },
	{ "PING", &executor_impl::exec_ping },
	{ "EXPIRE", &executor_impl::exec_expire },
	{ "PEXPIRE", &executor_impl::exec_pexpire },
      }
{
}

resp::data
executor_impl::execute (resp::data resp)
{
  if (!resp.is<resp::array> ())
    return simple_error ("bad command: not array");

  auto &arr = resp.get<resp::array> ();
  if (!arr.has_value ())
    return simple_error ("bad command: null array");

  auto &vec = arr.value ();
  if (vec.empty ())
    return simple_error ("bad command: empty array");

  auto validator{ [] (const resp::data &resp) {
    auto p = resp.get_if<resp::bulk_string> ();
    return p && p->has_value ();
  } };
  if (!std::all_of (vec.begin (), vec.end (), validator))
    return simple_error ("bad command: invalid arguments");

  auto &cmd = vec.front ().get<resp::bulk_string> ().value ();

  args_.clear ();
  args_.reserve (vec.size () - 1);
  for (auto it = vec.begin () + 1; it != vec.end (); it++)
    args_.push_back (it->get<resp::bulk_string> ().value ());

  return exec (cmd);
}

resp::data
executor_impl::exec (string_view cmd)
{
  auto it = cmds_.find (cmd);
  if (it != cmds_.end ())
    {
      auto fn = it->second;
      return (this->*fn) ();
    }
  return simple_error ("bad command: unknown command");
}

resp::data
executor_impl::exec_set ()
{
  // SET key value [NX | XX] [GET] [EX seconds | PX milliseconds |
  //   EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]

  // RETURN:
  // if GET was not specified:
  //   - nil: Operation was aborted (conflict with one of the XX/NX options).
  //          The key was not set.
  //   - simple string: OK: The key was set.
  // if GET was specified:
  //   - nil: The key didn't exist before the SET. If XX was specified, the key
  //          was not set. Otherwise, the key was set.
  //   - bulk string: The previous value of the key. If NX was specified, the
  //                  key was not set. Otherwise, the key was set.

  if (args_.size () < 2)
    return simple_error ("bad command: invalid arguments");

  auto &s0 = args_[0].get ();
  auto &s1 = args_[1].get ();

  // TODO: support expires

  ttl_.erase (s0);

  storage::data data{ storage::raw{ std::move (s1) } };
  db_.insert_or_assign (std::move (s0), std::move (data));

  return simple_string ("OK");
}

resp::data
executor_impl::exec_get ()
{
  // GET key

  // RETURN:
  // - bulk string: the value of the key.
  // - nil: if the key does not exist.

  if (args_.size () != 1)
    return simple_error ("bad command: invalid arguments");

  auto &s0 = args_[0].get ();

  auto it = db_find (s0);
  if (it == db_.end ())
    return null_bulk_string ();

  auto &data = it->second;
  if (!data.is<storage::raw> ())
    return simple_error ("WRONGTYPE");

  const auto &str = data.get<storage::raw> ();
  return bulk_string (str);
}

resp::data
executor_impl::exec_del ()
{
  // DEL key [key ...]

  // RETURN:
  // - integer: the number of keys that were removed.

  if (args_.size () < 1)
    return simple_error ("bad command: invalid arguments");

  std::int64_t num = 0;
  for (auto ref : args_)
    {
      const auto &key = ref.get ();
      auto it = db_find (key);
      if (it != db_.end ())
	{
	  db_erase (it);
	  num++;
	}
    }

  return integer (num);
}

resp::data
executor_impl::exec_ping ()
{
  // PING [message]

  // RETURN:
  // - simple string: PONG when no argument is provided.
  // - bulk string: the provided argument.

  switch (args_.size ())
    {
    case 0:
      return simple_string ("PONG");

    case 1:
      {
	auto &s0 = args_[0].get ();
	return resp::data{ resp::bulk_string{ std::move (s0) } };
      }

    default:
      return simple_error ("bad command: invalid arguments");
    }
}

resp::data
executor_impl::exec_expire ()
{
  // EXPIRE key seconds [NX | XX | GT | LT]

  // RETURN:
  // - integer: 0 if the timeout was not set; for example, the key doesn't
  //            exist, or the operation was skipped because of the provided
  //            arguments.
  // - integer: 1 if the timeout was set.

  auto bad = integer (0);
  auto win = integer (1);

  if (args_.size () < 2)
    return simple_error ("bad command: invalid arguments");

  auto &s0 = args_[0].get ();
  auto &s1 = args_[1].get ();

  auto it = db_find (s0);
  if (it == db_.end ())
    return bad;

  std::int64_t secs;
  if (!try_lexical_convert (s1, secs))
    return bad;

  if (secs <= 0)
    {
      db_erase (it);
      return win;
    }

  auto expire = clock_type::now () + seconds (secs);
  ttl_.insert_or_assign (std::move (s0), expire);

  return win;
}

resp::data
executor_impl::exec_pexpire ()
{
  // PEXPIRE key milliseconds [NX | XX | GT | LT]

  // RETURN:
  // - integer: 0 if the timeout was not set. For example, if the key doesn't
  //            exist, or the operation skipped because of the provided
  //            arguments.
  // - integer: 1 if the timeout was set.

  auto bad = integer (0);
  auto win = integer (1);

  if (args_.size () < 2)
    return simple_error ("bad command: invalid arguments");

  auto &s0 = args_[0].get ();
  auto &s1 = args_[1].get ();

  auto it = db_find (s0);
  if (it == db_.end ())
    return bad;

  std::int64_t msecs;
  if (!try_lexical_convert (s1, msecs))
    return bad;

  if (msecs <= 0)
    {
      db_erase (it);
      return win;
    }

  auto expire = clock_type::now () + milliseconds (msecs);
  ttl_.insert_or_assign (std::move (s0), expire);

  return win;
}

auto
executor_impl::db_find (const std::string &key) -> executor_impl::db_iterator
{
  auto db_it = db_.find (key);
  if (db_it == db_.end ())
    return db_it;

  auto ttl_it = ttl_.find (key);
  if (ttl_it == ttl_.end ())
    return db_it;

  auto expire = ttl_it->second;
  auto now = clock_type::now ();
  if (now < expire)
    return db_it;

  db_erase (db_it);
  return db_.end ();
}

void
executor_impl::db_erase (db_iterator it)
{
  BOOST_ASSERT (it != db_.end ());
  auto &key = it->first;
  ttl_.erase (key);
  db_.erase (it);
}

} // namespace mini_redis
