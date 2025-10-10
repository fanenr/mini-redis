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
      }
{
}

resp::data
executor_impl::execute (resp::data resp)
{
  if (!resp.is<resp::array> ())
    return simple_error ("bad command: not an array");

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
  if (args_.size () != 2)
    return simple_error ("bad command: argument count mismatch");

  auto &s0 = args_[0].get ();
  auto &s1 = args_[1].get ();
  storage::data data{ storage::raw{ std::move (s1) } };
  db_.insert_or_assign (std::move (s0), std::move (data));

  // RETURN:
  // if GET was not specified:
  //   - nil: Operation was aborted (conflict with one of the XX/NX options).
  //          The key was not set.
  //   - simple string: OK: The key was set.
  return simple_string ("OK");

  // if GET was specified:
  //   - nil: The key didn't exist before the SET. If XX was specified, the key
  //          was not set. Otherwise, the key was set.
  //   - bulk string: The previous value of the key. If NX was specified, the
  //                  key was not set. Otherwise, the key was set.
}

resp::data
executor_impl::exec_get ()
{
  // GET key
  if (args_.size () != 1)
    return simple_error ("bad command: argument count mismatch");

  auto &s0 = args_[0].get ();
  auto it = db_.find (s0);

  // RETURN:
  // - bulk string: the value of the key.
  // - nil: if the key does not exist.
  if (it != db_.end ())
    {
      auto &data = it->second;
      if (data.is<storage::raw> ())
	return data.to_resp ();
      else
	return simple_error ("WRONGTYPE");
    }
  else
    return null_bulk_string ();
}

resp::data
executor_impl::exec_del ()
{
  // DEL key [key ...]
  if (args_.size () < 1)
    return simple_error ("bad command: argument count mismatch");

  std::int64_t num = 0;
  for (auto arg : args_)
    {
      const auto &key = arg.get ();
      auto it = db_.find (key);
      if (it != db_.end ())
	{
	  db_.erase (it);
	  num++;
	}
    }

  // RETURN:
  // - integer: the number of keys that were removed.
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
      return simple_error ("bad command: argument count mismatch");
    }
}

} // namespace mini_redis
