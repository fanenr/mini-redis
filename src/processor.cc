#include "processor.h"

namespace mini_redis
{

static inline resp::data
simple_string (std::string msg)
{
  return { resp::simple_string{ std::move (msg) } };
}

static inline resp::data
simple_error (std::string msg)
{
  return { resp::simple_error{ std::move (msg) } };
}

static inline resp::data
bulk_string (std::string str)
{
  return { resp::bulk_string{ std::move (str) } };
}

static inline resp::data
null_bulk_string ()
{
  return { resp::bulk_string{ boost::none } };
}

static inline resp::data
integer (std::int64_t num)
{
  return { resp::integer{ num } };
}

static inline resp::data
err_protocol ()
{
  return simple_error ("ERR Protocol error: expected array of bulk strings");
}

static inline resp::data
err_syntax ()
{
  return simple_error ("ERR syntax error");
}

static inline resp::data
err_bad_integer ()
{
  return simple_error ("ERR value is not an integer or out of range");
}

static inline resp::data
err_wrong_type ()
{
  return simple_error (
      "WRONGTYPE Operation against a key holding the wrong kind of value");
}

static inline resp::data
err_wrong_num_args (string_view cmd)
{
  std::string msg{ "ERR wrong number of arguments for '" };
  msg.append (cmd.data (), cmd.size ());
  msg += "' command";
  return simple_error (std::move (msg));
}

static inline resp::data
err_unknown_command (std::string cmd)
{
  std::string msg{ "ERR unknown command '" };
  msg.append (cmd.data (), cmd.size ());
  msg += "'";
  return simple_error (std::move (msg));
}

} // namespace mini_redis

namespace mini_redis
{

processor::processor (config &cfg) : config_{ cfg } {}

resp::data
processor::execute (resp::data resp)
{
  typedef resp::data (processor::*exec_fn) ();
  static const unordered_flat_map<string_view, exec_fn> exec_map{
    { "ping", &processor::exec_ping },

    { "set", &processor::exec_set },
    { "get", &processor::exec_get },
    { "del", &processor::exec_del },

    { "expire", &processor::exec_expire },
    { "pexpire", &processor::exec_pexpire },
    { "expireat", &processor::exec_expireat },
    { "pexpireat", &processor::exec_pexpireat },

    { "ttl", &processor::exec_ttl },
    { "pttl", &processor::exec_pttl },

    { "incr", &processor::exec_incr },
    { "incrby", &processor::exec_incrby },
    { "decr", &processor::exec_decr },
    { "decrby", &processor::exec_decrby },
  };

  if (!resp.is<resp::array> ())
    return err_protocol ();

  auto &arr = resp.get<resp::array> ();
  if (!arr.has_value ())
    return err_protocol ();

  auto &vec = arr.value ();
  if (vec.empty ())
    return err_protocol ();

  bool all_str = true;
  for (const auto &v : vec)
    {
      auto p = v.get_if<resp::bulk_string> ();
      if (p == nullptr || !p->has_value ())
	{
	  all_str = false;
	  break;
	}
    }
  if (!all_str)
    return err_protocol ();

  args_.clear ();
  args_.reserve (vec.size () - 1);
  for (auto it = vec.begin () + 1; it != vec.end (); it++)
    args_.push_back (std::move (it->get<resp::bulk_string> ().value ()));

  const auto &cmd_raw = vec[0].get<resp::bulk_string> ().value ();
  auto cmd = cmd_raw;
  boost::to_lower (cmd);
  auto it = exec_map.find (cmd);
  if (it == exec_map.end ())
    return err_unknown_command (cmd_raw);

  auto fn = it->second;
  return (this->*fn) ();
}

resp::data
processor::exec_ping ()
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
      return bulk_string (std::move (args_[0]));

    default:
      return err_wrong_num_args ("ping");
    }
}

resp::data
processor::exec_set ()
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
    return err_wrong_num_args ("set");

  bool nx = false;
  bool xx = false;
  bool get = false;
  bool ex = false;
  bool px = false;
  bool exat = false;
  bool pxat = false;
  bool keepttl = false;
  std::int64_t n = 0;

  for (std::size_t i = 2; i < args_.size (); i++)
    {
      auto &str = args_[i];
      boost::to_lower (str);
      if (str == "nx")
	{
	  if (nx || xx)
	    return err_syntax ();
	  nx = true;
	}
      else if (str == "xx")
	{
	  if (nx || xx)
	    return err_syntax ();
	  xx = true;
	}
      else if (str == "get")
	{
	  if (get)
	    return err_syntax ();
	  get = true;
	}
      else if (str == "keepttl")
	{
	  if (ex || px || exat || pxat || keepttl)
	    return err_syntax ();
	  keepttl = true;
	}
      else if (str == "ex" || str == "px" || str == "exat" || str == "pxat")
	{
	  if (ex || px || exat || pxat || keepttl)
	    return err_syntax ();

	  if (str == "ex")
	    ex = true;
	  else if (str == "px")
	    px = true;
	  else if (str == "exat")
	    exat = true;
	  else if (str == "pxat")
	    pxat = true;

	  if (++i >= args_.size ())
	    return err_syntax ();

	  const auto &num = args_[i];
	  if (!try_lexical_convert (num, n) || n <= 0)
	    return err_bad_integer ();
	}
      else
	return err_syntax ();
    }

  auto &key = args_[0];
  auto opt_it = storage_.find (key);
  bool exists = opt_it.has_value ();
  resp::data old = null_bulk_string ();

  if (get && exists)
    {
      const auto &data = opt_it.value ()->second;
      if (data.is<db::string> ())
	{
	  const auto &str = data.get<db::string> ();
	  old = bulk_string (str);
	}
      else if (data.is<db::integer> ())
	{
	  const auto &num = data.get<db::integer> ();
	  old = bulk_string (lexical_cast<std::string> (num));
	}
      else
	return err_wrong_type ();
    }

  if (nx && exists)
    return get ? old : null_bulk_string ();
  if (xx && !exists)
    return get ? old : null_bulk_string ();

  auto &value = args_[1];
  db::data data{ db::string{ std::move (value) } };
  auto it = storage_.insert (std::move (key), std::move (data));

  if (ex)
    storage_.expire_after (it, seconds (n));
  else if (px)
    storage_.expire_after (it, milliseconds (n));
  else if (exat)
    {
      db::storage::time_point tp{ seconds{ n } };
      storage_.expire_at (it, tp);
    }
  else if (pxat)
    {
      db::storage::time_point tp{ milliseconds{ n } };
      storage_.expire_at (it, tp);
    }
  else if (!keepttl)
    storage_.clear_expires (it);

  return get ? old : simple_string ("OK");
}

resp::data
processor::exec_get ()
{
  // GET key

  // RETURN:
  // - bulk string: the value of the key.
  // - nil: if the key does not exist.

  if (args_.size () != 1)
    return err_wrong_num_args ("get");

  const auto &key = args_[0];

  auto opt_it = storage_.find (key);
  if (!opt_it.has_value ())
    return null_bulk_string ();

  const auto &data = opt_it.value ()->second;
  if (data.is<db::string> ())
    {
      const auto &str = data.get<db::string> ();
      return bulk_string (str);
    }
  else if (data.is<db::integer> ())
    {
      const auto &num = data.get<db::integer> ();
      auto str = lexical_cast<std::string> (num);
      return bulk_string (std::move (str));
    }

  return err_wrong_type ();
}

resp::data
processor::exec_del ()
{
  // DEL key [key ...]

  // RETURN:
  // - integer: the number of keys that were removed.

  if (args_.size () < 1)
    return err_wrong_num_args ("del");

  std::int64_t n = 0;
  for (const auto &key : args_)
    {
      auto opt_it = storage_.find (key);
      if (opt_it.has_value ())
	{
	  storage_.erase (opt_it.value ());
	  n++;
	}
    }

  return integer (n);
}

resp::data
processor::exec_expire ()
{
  // EXPIRE key seconds [NX | XX | GT | LT]

  // RETURN:
  // - integer: 0 if the timeout was not set; for example, the key doesn't
  //            exist, or the operation was skipped because of the provided
  //            arguments.
  // - integer: 1 if the timeout was set.

  return expire_impl<seconds, false> ("expire");
}

resp::data
processor::exec_pexpire ()
{
  // PEXPIRE key milliseconds [NX | XX | GT | LT]

  // RETURN:
  // - integer: 0 if the timeout was not set. For example, if the key doesn't
  //            exist, or the operation skipped because of the provided
  //            arguments.
  // - integer: 1 if the timeout was set.

  return expire_impl<milliseconds, false> ("pexpire");
}

resp::data
processor::exec_expireat ()
{
  // EXPIREAT key unix-time-seconds [NX | XX | GT | LT]

  // RETURN:
  // - integer: 0 if the timeout was not set; for example, the key doesn't
  //            exist, or the operation was skipped because of the provided
  //            arguments.
  // - integer: 1 if the timeout was set.

  return expire_impl<seconds, true> ("expireat");
}

resp::data
processor::exec_pexpireat ()
{
  // PEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT]

  // RETURN:
  // - integer: 0 if the timeout was not set. For example, if the key doesn't
  //            exist, or the operation skipped because of the provided
  //            arguments.
  // - integer: 1 if the timeout was set.

  return expire_impl<milliseconds, true> ("pexpireat");
}

template <class Duration, bool At>
resp::data
processor::expire_impl (string_view cmd)
{
  if (args_.size () != 2)
    return err_wrong_num_args (cmd);

  std::int64_t n;
  const auto &num = args_[1];
  if (!try_lexical_convert (num, n))
    return err_bad_integer ();

  const auto &key = args_[0];
  auto opt_it = storage_.find (key);
  if (!opt_it.has_value ())
    return integer (0);

  auto it = opt_it.value ();
  db::storage::time_point expires;
  auto now = db::storage::clock_type::now ();

  if (At)
    expires = db::storage::time_point{ Duration{ n } };
  else
    expires = now + Duration{ n };

  if (expires <= now)
    {
      storage_.erase (it);
      return integer (0);
    }
  else
    storage_.expire_at (it, expires);

  return integer (1);
}

resp::data
processor::exec_ttl ()
{
  // TTL key

  // RETURN:
  // - integer: TTL in seconds.
  // - integer: -1 if the key exists but has no associated expiration.
  // - integer: -2 if the key does not exist.

  return ttl_impl<seconds> ("ttl");
}

resp::data
processor::exec_pttl ()
{
  // PTTL key

  // RETURN:
  // - integer: TTL in milliseconds.
  // - integer: -1 if the key exists but has no associated expiration.
  // - integer: -2 if the key does not exist.

  return ttl_impl<milliseconds> ("pttl");
}

template <class Duration>
resp::data
processor::ttl_impl (string_view cmd)
{
  if (args_.size () != 1)
    return err_wrong_num_args (cmd);

  const auto &key = args_[0];
  auto opt_it = storage_.find (key);
  if (!opt_it.has_value ())
    return integer (-2);

  auto it = opt_it.value ();

  auto ttl = storage_.ttl (it);
  if (!ttl.has_value ())
    return integer (-1);

  auto n = duration_cast<Duration> (ttl.value ()).count ();
  if (n <= 0)
    {
      storage_.erase (it);
      return integer (-2);
    }

  return integer (n);
}

resp::data
processor::exec_incr ()
{
  // INCR key

  // RETURN:
  // - integer: the value of the key after the increment.

  return calc_impl<std::plus> ("incr", false);
}

resp::data
processor::exec_incrby ()
{
  // INCRBY key increment

  // RETURN:
  // - integer: the value of the key after the increment.

  return calc_impl<std::plus> ("incrby", true);
}

resp::data
processor::exec_decr ()
{
  // DECR key

  // RETURN:
  // - integer: the value of the key after decrementing it.

  return calc_impl<std::minus> ("decr", false);
}

resp::data
processor::exec_decrby ()
{
  // DECRBY key decrement

  // RETURN:
  // - integer: the value of the key after decrementing it.

  return calc_impl<std::minus> ("decrby", true);
}

template <template <class> class Op>
resp::data
processor::calc_impl (string_view cmd, bool with_rhs)
{
  if ((!with_rhs && args_.size () != 1) || (with_rhs && args_.size () != 2))
    return err_wrong_num_args (cmd);

  auto &key = args_[0];
  std::int64_t rhs = 1;
  if (with_rhs)
    {
      const auto &num = args_[1];
      if (!try_lexical_convert (num, rhs))
	return err_bad_integer ();
    }

  Op<std::int64_t> oper{};

  auto opt_it = storage_.find (key);
  if (!opt_it.has_value ())
    {
      auto n = oper (0, rhs);
      db::data data{ db::integer{ n } };
      storage_.insert (std::move (key), std::move (data));
      return integer (n);
    }

  auto it = opt_it.value ();
  auto &data = it->second;
  if (data.is<db::integer> ())
    {
      auto &n = data.get<db::integer> ();
      n = oper (n, rhs);
      return integer (n);
    }
  else if (data.is<db::string> ())
    {
      std::int64_t n;
      const auto &num = data.get<db::string> ();
      if (!try_lexical_convert (num, n))
	return err_bad_integer ();

      n = oper (n, rhs);
      data = db::data{ db::integer{ n } };
      return integer (n);
    }
  else
    return err_wrong_type ();
}

} // namespace mini_redis
