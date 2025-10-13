#include "processor.h"

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

const auto invalid_arguments = simple_error ("invalid arguments");
const auto wrong_type = simple_error ("wrong type");

} // namespace mini_redis

namespace mini_redis
{

processor::processor (config &cfg) : config_ (cfg) {}

resp::data
processor::execute (resp::data resp)
{
  if (!resp.is<resp::array> ())
    return simple_error ("not array");

  auto &arr = resp.get<resp::array> ();
  if (!arr.has_value ())
    return simple_error ("null array");

  auto &vec = arr.value ();
  if (vec.empty ())
    return simple_error ("empty array");

  auto validator{ [] (const resp::data &resp) {
    auto p = resp.get_if<resp::bulk_string> ();
    return p && p->has_value ();
  } };
  if (!std::all_of (vec.begin (), vec.end (), validator))
    return simple_error ("invalid arguments");

  args_.clear ();
  args_.reserve (vec.size () - 1);
  for (auto it = vec.begin () + 1; it != vec.end (); it++)
    args_.push_back (it->get<resp::bulk_string> ().value ());

  const auto &cmd = vec[0].get<resp::bulk_string> ().value ();
  auto it = exec_map_.find (cmd);
  if (it != exec_map_.end ())
    {
      auto fn = it->second;
      return (this->*fn) ();
    }

  return simple_error ("unknown command");
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
      {
	auto &msg = args_[0].get ();
	return resp::data{ resp::bulk_string{ std::move (msg) } };
      }

    default:
      return invalid_arguments;
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
    return invalid_arguments;

  auto &key = args_[0].get ();
  auto &value = args_[1].get ();

  bool nx = false;
  bool xx = false;
  bool get = false;
  bool ex = false;
  bool px = false;
  bool exat = false;
  bool pxat = false;
  bool keepttl = false;
  std::int64_t num = 0;

  for (std::size_t i = 2; i < args_.size (); i++)
    {
      const auto &str = args_[i].get ();
      if (str == "NX")
	{
	  if (nx || xx)
	    return invalid_arguments;
	  nx = true;
	}
      else if (str == "XX")
	{
	  if (nx || xx)
	    return invalid_arguments;
	  xx = true;
	}
      else if (str == "GET")
	{
	  if (get)
	    return invalid_arguments;
	  get = true;
	}
      else if (str == "KEEPTTL")
	{
	  if (ex || px || exat || pxat || keepttl)
	    return invalid_arguments;
	  keepttl = true;
	}
      else if (str == "EX" || str == "PX" || str == "EXAT" || str == "PXAT")
	{
	  if (ex || px || exat || pxat || keepttl)
	    return invalid_arguments;

	  if (str == "EX")
	    ex = true;
	  else if (str == "PX")
	    px = true;
	  else if (str == "EXAT")
	    exat = true;
	  else if (str == "PXAT")
	    pxat = true;

	  i++;
	  if (i >= args_.size ())
	    return invalid_arguments;

	  const auto &str = args_[i].get ();
	  if (!try_lexical_convert (str, num) || num <= 0)
	    return invalid_arguments;
	}
      else
	return invalid_arguments;
    }

  auto it = storage_.find (key);
  bool exists = (it != storage_.end ());
  resp::data old = null_bulk_string ();

  if (get && exists)
    {
      const auto &data = it->second;
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
	return wrong_type;
    }

  if (nx && exists)
    return get ? old : null_bulk_string ();
  if (xx && !exists)
    return get ? old : null_bulk_string ();

  db::data data{ db::string{ std::move (value) } };
  it = storage_.insert (std::move (key), std::move (data));

  if (ex)
    storage_.expire_after (it, seconds (num));
  else if (px)
    storage_.expire_after (it, milliseconds (num));
  else if (exat)
    {
      auto tp = db::storage::time_point{ seconds{ num } };
      storage_.expire_at (it, tp);
    }
  else if (pxat)
    {
      auto tp = db::storage::time_point{ milliseconds{ num } };
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
    return invalid_arguments;

  auto &key = args_[0].get ();

  auto it = storage_.find (key);
  if (it == storage_.end ())
    return null_bulk_string ();

  auto &data = it->second;
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

  return wrong_type;
}

resp::data
processor::exec_del ()
{
  // DEL key [key ...]

  // RETURN:
  // - integer: the number of keys that were removed.

  if (args_.size () < 1)
    return invalid_arguments;

  std::int64_t num = 0;
  for (auto ref : args_)
    {
      const auto &key = ref.get ();
      auto it = storage_.find (key);
      if (it != storage_.end ())
	{
	  storage_.erase (it);
	  num++;
	}
    }

  return integer (num);
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

  return generic_expire<seconds, false> ();
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

  return generic_expire<milliseconds, false> ();
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

  return generic_expire<seconds, true> ();
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

  return generic_expire<milliseconds, true> ();
}

template <class Duration, bool At>
resp::data
processor::generic_expire ()
{
  if (args_.size () != 2)
    return invalid_arguments;

  auto bad = integer (0);
  auto win = integer (1);

  auto &key = args_[0].get ();
  auto &num = args_[1].get ();

  auto it = storage_.find (key);
  if (it == storage_.end ())
    return bad;

  std::int64_t n;
  if (!try_lexical_convert (num, n))
    return bad;

  db::storage::time_point expires;
  auto now = db::storage::clock_type::now ();

  if (At)
    expires = db::storage::time_point{ Duration{ n } };
  else
    expires = now + Duration{ n };

  if (expires <= now)
    {
      storage_.erase (it);
      return bad;
    }

  storage_.expire_at (it, expires);

  return win;
}

resp::data
processor::exec_ttl ()
{
  // TTL key

  // RETURN:
  // - integer: TTL in seconds.
  // - integer: -1 if the key exists but has no associated expiration.
  // - integer: -2 if the key does not exist.

  return generic_ttl<seconds> ();
}

resp::data
processor::exec_pttl ()
{
  // PTTL key

  // RETURN:
  // - integer: TTL in milliseconds.
  // - integer: -1 if the key exists but has no associated expiration.
  // - integer: -2 if the key does not exist.

  return generic_ttl<milliseconds> ();
}

template <class Duration>
resp::data
processor::generic_ttl ()
{
  if (args_.size () != 1)
    return invalid_arguments;

  auto &key = args_[0].get ();

  auto it = storage_.find (key);
  if (it == storage_.end ())
    return integer (-2);

  auto ttl = storage_.ttl (it);
  if (!ttl)
    return integer (-1);

  auto num = duration_cast<Duration> (*ttl).count ();
  if (num <= 0)
    {
      storage_.erase (it);
      return integer (-2);
    }

  return integer (num);
}

resp::data
processor::exec_incr ()
{
  // INCR key

  // RETURN:
  // - integer: the value of the key after the increment.

  return generic_calc<std::plus> ();
}

resp::data
processor::exec_incrby ()
{
  // INCRBY key increment

  // RETURN:
  // - integer: the value of the key after the increment.

  return generic_calc<std::plus> ();
}

resp::data
processor::exec_decr ()
{
  // DECR key

  // RETURN:
  // - integer: the value of the key after decrementing it.

  return generic_calc<std::minus> ();
}

resp::data
processor::exec_decrby ()
{
  // DECRBY key decrement

  // RETURN:
  // - integer: the value of the key after decrementing it.

  return generic_calc<std::minus> ();
}

template <template <class T> class Op>
resp::data
processor::generic_calc ()
{
  if (args_.size () < 1)
    return invalid_arguments;

  auto &key = args_[0].get ();
  std::int64_t rhs;

  switch (args_.size ())
    {
    case 1:
      rhs = 1;
      break;

    case 2:
      {
	auto &str = args_[1].get ();
	if (!try_lexical_convert (str, rhs))
	  return wrong_type;
      }
      break;

    default:
      return invalid_arguments;
    }

  auto oper = Op<std::int64_t>{};
  auto it = storage_.find (key);
  if (it == storage_.end ())
    {
      auto num = oper (0, rhs);
      db::data data{ db::integer{ num } };
      storage_.insert (std::move (key), std::move (data));
      return integer (num);
    }

  auto &data = it->second;
  if (data.is<db::integer> ())
    {
      auto &num = data.get<db::integer> ();
      num = oper (num, rhs);
      return integer (num);
    }
  else if (data.is<db::string> ())
    {
      const auto &str = data.get<db::string> ();

      std::int64_t num;
      if (!try_lexical_convert (str, num))
	return wrong_type;

      num = oper (num, rhs);
      data = db::data{ db::integer{ num } };
      return integer (num);
    }
  else
    return wrong_type;
}

} // namespace mini_redis
