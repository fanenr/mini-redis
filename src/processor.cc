#include "processor.h"
#include "db_disk.h"

namespace mini_redis
{
namespace
{

const char *default_dump_path = "dump.mrdb";

resp::data
integer (std::int64_t num)
{
  return { resp::integer{ num } };
}

resp::data
simple_error (std::string msg)
{
  return { resp::simple_error{ std::move (msg) } };
}

resp::data
simple_string (std::string msg)
{
  return { resp::simple_string{ std::move (msg) } };
}

resp::data
bulk_string (std::string str)
{
  return { resp::bulk_string{ std::move (str) } };
}

resp::data
array (std::vector<resp::data> items)
{
  return { resp::array{ std::move (items) } };
}

resp::data
null_bulk_string ()
{
  return { resp::bulk_string{ boost::none } };
}

resp::data
null_array ()
{
  return { resp::array{ boost::none } };
}

resp::data
empty_array ()
{
  return { resp::array{ std::vector<resp::data>{} } };
}

const resp::data e_protocol
    = simple_error ("ERR Protocol error: expected array of bulk strings");

const resp::data e_syntax = simple_error ("ERR syntax error");

const resp::data e_bad_integer
    = simple_error ("ERR value is not an integer or out of range");

const resp::data e_overflow
    = simple_error ("ERR increment or decrement would overflow");

const resp::data e_wrong_type = simple_error (
    "WRONGTYPE Operation against a key holding the wrong kind of value");

const resp::data e_no_such_key = simple_error ("ERR no such key");

const resp::data e_index_out_of_range
    = simple_error ("ERR index out of range");

const resp::data e_value_out_of_range_positive
    = simple_error ("ERR value is out of range, must be positive");

resp::data
e_wrong_num_args (string_view cmd)
{
  std::string msg{ "ERR wrong number of arguments for '" };
  msg.append (cmd.data (), cmd.size ());
  msg.append ("' command");
  return simple_error (std::move (msg));
}

resp::data
e_unknown_command (string_view cmd)
{
  std::string msg{ "ERR unknown command '" };
  msg.append (cmd.data (), cmd.size ());
  msg.append ("'");
  return simple_error (std::move (msg));
}

resp::data
e_persistence (std::string msg)
{
  std::string out{ "ERR " };
  out.append (msg);
  return simple_error (std::move (out));
}

template <class Op>
optional<std::int64_t>
checked_calc (std::int64_t lhs, std::int64_t rhs)
{
  static_assert (std::is_same<Op, std::plus<std::int64_t>>::value
		     || std::is_same<Op, std::minus<std::int64_t>>::value,
		 "checked_calc only supports plus/minus");

  const auto min = std::numeric_limits<std::int64_t>::min ();
  const auto max = std::numeric_limits<std::int64_t>::max ();
  const bool is_sub = std::is_same<Op, std::minus<std::int64_t>>::value;

  if (!is_sub)
    {
      if (rhs > 0 && lhs > max - rhs)
	return boost::none;
      if (rhs < 0 && lhs < min - rhs)
	return boost::none;
      return lhs + rhs;
    }

  if (rhs > 0 && lhs < min + rhs)
    return boost::none;
  if (rhs < 0 && lhs > max + rhs)
    return boost::none;
  return lhs - rhs;
}

std::int64_t
to_int64 (std::size_t n)
{
  auto max
      = static_cast<std::size_t> (std::numeric_limits<std::int64_t>::max ());
  if (n > max)
    return std::numeric_limits<std::int64_t>::max ();
  return static_cast<std::int64_t> (n);
}

optional<std::size_t>
normalize_lindex (std::int64_t index, std::size_t len)
{
  if (len == 0)
    return boost::none;

  auto len_i64 = to_int64 (len);
  if (index >= 0)
    {
      if (index >= len_i64)
	return boost::none;
      return static_cast<std::size_t> (index);
    }

  auto normalized = len_i64 + index;
  if (normalized < 0)
    return boost::none;
  return static_cast<std::size_t> (normalized);
}

optional<std::pair<std::size_t, std::size_t>>
normalize_lrange (std::int64_t start, std::int64_t stop, std::size_t len)
{
  if (len == 0)
    return boost::none;

  auto len_i64 = to_int64 (len);

  if (start < 0)
    start += len_i64;
  if (stop < 0)
    stop += len_i64;

  if (start < 0)
    start = 0;
  if (stop < 0)
    return boost::none;
  if (start >= len_i64)
    return boost::none;
  if (stop >= len_i64)
    stop = len_i64 - 1;
  if (start > stop)
    return boost::none;

  return std::pair<std::size_t, std::size_t>{
    static_cast<std::size_t> (start), static_cast<std::size_t> (stop)
  };
}

} // namespace

processor::processor (config &cfg) : config_{ cfg } {}

resp::data
processor::execute (resp::data resp)
{
  typedef resp::data (processor::*exec_fn) ();
  static const unordered_flat_map<string_view, exec_fn> exec_map{
    // Connection commands
    { "ping", &processor::exec_ping },

    // Server commands
    { "save", &processor::exec_save },
    { "load", &processor::exec_load },

    // String commands
    { "set", &processor::exec_set },
    { "get", &processor::exec_get },
    { "incr", &processor::exec_incr },
    { "incrby", &processor::exec_incrby },
    { "decr", &processor::exec_decr },
    { "decrby", &processor::exec_decrby },

    // Generic commands
    { "del", &processor::exec_del },
    { "expire", &processor::exec_expire },
    { "pexpire", &processor::exec_pexpire },
    { "expireat", &processor::exec_expireat },
    { "pexpireat", &processor::exec_pexpireat },
    { "ttl", &processor::exec_ttl },
    { "pttl", &processor::exec_pttl },

    // List commands
    { "llen", &processor::exec_llen },
    { "lindex", &processor::exec_lindex },
    { "lrange", &processor::exec_lrange },

    { "lset", &processor::exec_lset },
    { "lrem", &processor::exec_lrem },
    { "linsert", &processor::exec_linsert },

    { "lpush", &processor::exec_lpush },
    { "rpush", &processor::exec_rpush },
    { "lpop", &processor::exec_lpop },
    { "rpop", &processor::exec_rpop },
  };

  if (!resp.is<resp::array> ())
    return e_protocol;

  auto &arr = resp.get<resp::array> ();
  if (!arr.has_value ())
    return e_protocol;

  auto &vec = arr.value ();
  if (vec.empty ())
    return e_protocol;

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
    return e_protocol;

  args_.clear ();
  args_.reserve (vec.size () - 1);
  for (auto it = vec.begin () + 1; it != vec.end (); it++)
    args_.push_back (std::move (it->get<resp::bulk_string> ().value ()));

  const auto &cmd_raw = vec[0].get<resp::bulk_string> ().value ();
  auto cmd = cmd_raw;
  boost::to_lower (cmd);
  auto it = exec_map.find (cmd);
  if (it == exec_map.end ())
    return e_unknown_command (cmd_raw);

  auto fn = it->second;
  return (this->*fn) ();
}

// Connection commands
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
      return e_wrong_num_args ("ping");
    }
}

// Server commands
resp::data
processor::exec_save ()
{
  // SAVE [TO path]

  // RETURN:
  // - simple string: OK.

  std::string path{ default_dump_path };
  if (!args_.empty ())
    {
      if (args_.size () != 2)
	return e_syntax;

      auto opt = args_[0];
      boost::to_lower (opt);
      if (opt != "to")
	return e_syntax;

      path = std::move (args_[1]);
    }

  auto ret = db::save_to (path, storage_.create_snapshot ());
  if (!ret.has_value ())
    return e_persistence (ret.error ());

  return simple_string ("OK");
}

resp::data
processor::exec_load ()
{
  // LOAD [FROM path]

  // RETURN:
  // - simple string: OK.

  std::string path{ default_dump_path };
  if (!args_.empty ())
    {
      if (args_.size () != 2)
	return e_syntax;

      auto opt = args_[0];
      boost::to_lower (opt);
      if (opt != "from")
	return e_syntax;

      path = std::move (args_[1]);
    }

  db::snapshot snap;
  auto res = db::load_from (path, snap);
  if (!res.has_value ())
    return e_persistence (res.error ());

  storage_.replace_with_snapshot (std::move (snap));
  return simple_string ("OK");
}

// String commands
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
    return e_wrong_num_args ("set");

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
	    return e_syntax;
	  nx = true;
	}
      else if (str == "xx")
	{
	  if (nx || xx)
	    return e_syntax;
	  xx = true;
	}
      else if (str == "get")
	{
	  if (get)
	    return e_syntax;
	  get = true;
	}
      else if (str == "keepttl")
	{
	  if (ex || px || exat || pxat || keepttl)
	    return e_syntax;
	  keepttl = true;
	}
      else if (str == "ex" || str == "px" || str == "exat" || str == "pxat")
	{
	  if (ex || px || exat || pxat || keepttl)
	    return e_syntax;

	  if (str == "ex")
	    ex = true;
	  else if (str == "px")
	    px = true;
	  else if (str == "exat")
	    exat = true;
	  else if (str == "pxat")
	    pxat = true;

	  if (++i >= args_.size ())
	    return e_syntax;

	  const auto &num = args_[i];
	  if (!try_lexical_convert (num, n) || n <= 0)
	    return e_bad_integer;
	}
      else
	return e_syntax;
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
	return e_wrong_type;
    }

  if (nx && exists)
    return get ? old : null_bulk_string ();
  if (xx && !exists)
    return get ? old : null_bulk_string ();

  auto &value = args_[1];
  db::data data{ db::string{ std::move (value) } };
  auto it = storage_.insert (std::move (key), std::move (data));

  if (ex)
    storage_.expire_after (it, seconds{ n });
  else if (px)
    storage_.expire_after (it, milliseconds{ n });
  else if (exat)
    {
      db::time_point tp{ seconds{ n } };
      storage_.expire_at (it, tp);
    }
  else if (pxat)
    {
      db::time_point tp{ milliseconds{ n } };
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
    return e_wrong_num_args ("get");

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

  return e_wrong_type;
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
    return e_wrong_num_args (cmd);

  auto &key = args_[0];
  std::int64_t rhs = 1;
  if (with_rhs)
    {
      const auto &num = args_[1];
      if (!try_lexical_convert (num, rhs))
	return e_bad_integer;
    }

  auto calc = [rhs] (std::int64_t lhs) -> optional<std::int64_t>
    { return checked_calc<Op<std::int64_t>> (lhs, rhs); };

  auto opt_it = storage_.find (key);
  if (!opt_it.has_value ())
    {
      auto opt_n = calc (0);
      if (!opt_n.has_value ())
	return e_overflow;

      auto n = opt_n.value ();
      db::data data{ db::integer{ n } };
      storage_.insert (std::move (key), std::move (data));
      return integer (n);
    }

  auto it = opt_it.value ();
  auto &data = it->second;
  if (data.is<db::integer> ())
    {
      auto &n = data.get<db::integer> ();
      auto opt_n = calc (n);
      if (!opt_n.has_value ())
	return e_overflow;

      n = opt_n.value ();
      return integer (n);
    }
  else if (data.is<db::string> ())
    {
      std::int64_t n;
      const auto &num = data.get<db::string> ();
      if (!try_lexical_convert (num, n))
	return e_bad_integer;

      auto opt_n = calc (n);
      if (!opt_n.has_value ())
	return e_overflow;

      n = opt_n.value ();
      data = db::data{ db::integer{ n } };
      return integer (n);
    }
  else
    return e_wrong_type;
}

// Generic commands
resp::data
processor::exec_del ()
{
  // DEL key [key ...]

  // RETURN:
  // - integer: the number of keys that were removed.

  if (args_.size () < 1)
    return e_wrong_num_args ("del");

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
  if (args_.size () != 2 && args_.size () != 3)
    return e_wrong_num_args (cmd);

  enum
  {
    cond_none = 0,
    cond_nx,
    cond_xx,
    cond_gt,
    cond_lt,
  };

  auto cond = cond_none;
  if (args_.size () == 3)
    {
      auto opt = args_[2];
      boost::to_lower (opt);
      if (opt == "nx")
	cond = cond_nx;
      else if (opt == "xx")
	cond = cond_xx;
      else if (opt == "gt")
	cond = cond_gt;
      else if (opt == "lt")
	cond = cond_lt;
      else
	return e_syntax;
    }

  std::int64_t n;
  const auto &num = args_[1];
  if (!try_lexical_convert (num, n))
    return e_bad_integer;

  const auto &key = args_[0];
  auto opt_it = storage_.find (key);
  if (!opt_it.has_value ())
    return integer (0);

  auto it = opt_it.value ();
  db::time_point expires;
  auto now = db::clock_type::now ();
  auto ttl = storage_.ttl (it);
  auto zero = db::duration::zero ();
  if (ttl.has_value () && ttl.value () <= zero)
    {
      storage_.erase (it);
      return integer (0);
    }

  if (At)
    expires = db::time_point{ Duration{ n } };
  else
    expires = now + Duration{ n };

  auto new_ttl = expires - now;

  bool can_set = true;
  switch (cond)
    {
    case cond_none:
      break;

    case cond_nx:
      can_set = !ttl.has_value ();
      break;

    case cond_xx:
      can_set = ttl.has_value ();
      break;

    case cond_gt:
      can_set = ttl.has_value () && new_ttl > ttl.value ();
      break;

    case cond_lt:
      can_set = !ttl.has_value () || new_ttl < ttl.value ();
      break;

    default:
      BOOST_THROW_EXCEPTION (std::logic_error ("bad expire_cond"));
    }
  if (!can_set)
    return integer (0);

  if (new_ttl <= zero)
    {
      storage_.erase (it);
      return integer (1);
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
    return e_wrong_num_args (cmd);

  const auto &key = args_[0];
  auto opt_it = storage_.find (key);
  if (!opt_it.has_value ())
    return integer (-2);

  auto it = opt_it.value ();

  auto ttl = storage_.ttl (it);
  if (!ttl.has_value ())
    return integer (-1);

  auto ttl_raw = ttl.value ();
  if (ttl_raw <= db::duration::zero ())
    {
      storage_.erase (it);
      return integer (-2);
    }

  auto n = duration_cast<Duration> (ttl_raw).count ();
  return integer (n);
}

// List commands
resp::data
processor::exec_llen ()
{
  // LLEN key

  // RETURN:
  // - integer: the length of the list.

  if (args_.size () != 1)
    return e_wrong_num_args ("llen");

  const auto &key = args_[0];
  auto opt_it = storage_.find (key);
  if (!opt_it.has_value ())
    return integer (0);

  const auto &data = opt_it.value ()->second;
  if (!data.is<db::list> ())
    return e_wrong_type;

  return integer (to_int64 (data.get<db::list> ().size ()));
}

resp::data
processor::exec_lindex ()
{
  // LINDEX key index

  // RETURN:
  // - nil: when index is out of range.
  // - bulk string: the requested element.

  if (args_.size () != 2)
    return e_wrong_num_args ("lindex");

  std::int64_t index;
  if (!try_lexical_convert (args_[1], index))
    return e_bad_integer;

  const auto &key = args_[0];
  auto opt_it = storage_.find (key);
  if (!opt_it.has_value ())
    return null_bulk_string ();

  const auto &data = opt_it.value ()->second;
  if (!data.is<db::list> ())
    return e_wrong_type;

  const auto &ls = data.get<db::list> ();
  auto opt_pos = normalize_lindex (index, ls.size ());
  if (!opt_pos.has_value ())
    return null_bulk_string ();

  return bulk_string (ls[opt_pos.value ()]);
}

resp::data
processor::exec_lrange ()
{
  // LRANGE key start stop

  // RETURN:
  // - array: a list of elements in the specified range,
  //          or an empty array if the key doesn't exist.

  if (args_.size () != 3)
    return e_wrong_num_args ("lrange");

  std::int64_t start;
  std::int64_t stop;
  if (!try_lexical_convert (args_[1], start)
      || !try_lexical_convert (args_[2], stop))
    return e_bad_integer;

  const auto &key = args_[0];
  auto opt_it = storage_.find (key);
  if (!opt_it.has_value ())
    return empty_array ();

  const auto &data = opt_it.value ()->second;
  if (!data.is<db::list> ())
    return e_wrong_type;

  const auto &ls = data.get<db::list> ();
  auto range = normalize_lrange (start, stop, ls.size ());
  if (!range.has_value ())
    return empty_array ();

  const auto first = range.value ().first;
  const auto last = range.value ().second;
  std::vector<resp::data> out;
  out.reserve (last - first + 1);
  for (auto i = first; i <= last; i++)
    out.push_back (bulk_string (ls[i]));

  return array (std::move (out));
}

resp::data
processor::exec_lset ()
{
  // LSET key index element

  // RETURN:
  // - simple string: OK.

  if (args_.size () != 3)
    return e_wrong_num_args ("lset");

  std::int64_t index;
  if (!try_lexical_convert (args_[1], index))
    return e_bad_integer;

  const auto &key = args_[0];
  auto opt_it = storage_.find (key);
  if (!opt_it.has_value ())
    return e_no_such_key;

  auto it = opt_it.value ();
  auto &data = it->second;
  if (!data.is<db::list> ())
    return e_wrong_type;

  auto &ls = data.get<db::list> ();
  auto opt_pos = normalize_lindex (index, ls.size ());
  if (!opt_pos.has_value ())
    return e_index_out_of_range;

  ls[opt_pos.value ()] = std::move (args_[2]);
  return simple_string ("OK");
}

resp::data
processor::exec_lrem ()
{
  // LREM key count element

  // RETURN:
  // - integer: the number of removed elements.

  if (args_.size () != 3)
    return e_wrong_num_args ("lrem");

  std::int64_t count;
  if (!try_lexical_convert (args_[1], count))
    return e_bad_integer;

  const auto &key = args_[0];
  auto opt_it = storage_.find (key);
  if (!opt_it.has_value ())
    return integer (0);

  auto it = opt_it.value ();
  auto &data = it->second;
  if (!data.is<db::list> ())
    return e_wrong_type;

  const auto &element = args_[2];
  auto &ls = data.get<db::list> ();

  std::int64_t removed = 0;
  if (count == 0)
    {
      for (auto iter = ls.begin (); iter != ls.end ();)
	{
	  if (*iter == element)
	    {
	      iter = ls.erase (iter);
	      removed++;
	    }
	  else
	    ++iter;
	}
    }
  else if (count > 0)
    {
      for (auto iter = ls.begin (); iter != ls.end () && removed < count;)
	{
	  if (*iter == element)
	    {
	      iter = ls.erase (iter);
	      removed++;
	    }
	  else
	    ++iter;
	}
    }
  else
    {
      auto limit = count == std::numeric_limits<std::int64_t>::min ()
		       ? std::numeric_limits<std::int64_t>::max ()
		       : -count;

      for (auto iter = ls.end (); iter != ls.begin () && removed < limit;)
	{
	  --iter;
	  if (*iter == element)
	    {
	      iter = ls.erase (iter);
	      removed++;
	    }
	}
    }

  if (ls.empty ())
    storage_.erase (it);

  return integer (removed);
}

resp::data
processor::exec_linsert ()
{
  // LINSERT key <BEFORE | AFTER> pivot element

  // RETURN:
  // - integer: the list length after a successful insert operation.
  // - integer: 0 when the key doesn't exist.
  // - integer: -1 when the pivot wasn't found.

  if (args_.size () != 4)
    return e_wrong_num_args ("linsert");

  auto where = args_[1];
  boost::to_lower (where);

  bool before = false;
  if (where == "before")
    before = true;
  else if (where != "after")
    return e_syntax;

  const auto &key = args_[0];
  auto opt_it = storage_.find (key);
  if (!opt_it.has_value ())
    return integer (0);

  auto it = opt_it.value ();
  auto &data = it->second;
  if (!data.is<db::list> ())
    return e_wrong_type;

  auto &ls = data.get<db::list> ();

  auto pos = ls.end ();
  for (auto iter = ls.begin (); iter != ls.end (); ++iter)
    if (*iter == args_[2])
      {
	pos = iter;
	break;
      }

  if (pos == ls.end ())
    return integer (-1);

  if (!before)
    ++pos;
  ls.insert (pos, std::move (args_[3]));
  return integer (to_int64 (ls.size ()));
}

resp::data
processor::exec_lpush ()
{
  // LPUSH key element [element ...]

  // RETURN:
  // - integer: the length of the list after the push operation.

  if (args_.size () < 2)
    return e_wrong_num_args ("lpush");

  auto &key = args_[0];
  auto opt_it = storage_.find (key);

  db::storage::iterator it;
  if (!opt_it.has_value ())
    {
      db::data data{ db::list{} };
      it = storage_.insert (std::move (key), std::move (data));
    }
  else
    {
      it = opt_it.value ();
      if (!it->second.is<db::list> ())
	return e_wrong_type;
    }

  auto &ls = it->second.get<db::list> ();
  for (std::size_t i = 1; i < args_.size (); i++)
    ls.push_front (std::move (args_[i]));

  return integer (to_int64 (ls.size ()));
}

resp::data
processor::exec_rpush ()
{
  // RPUSH key element [element ...]

  // RETURN:
  // - integer: the length of the list after the push operation.

  if (args_.size () < 2)
    return e_wrong_num_args ("rpush");

  auto &key = args_[0];
  auto opt_it = storage_.find (key);

  db::storage::iterator it;
  if (!opt_it.has_value ())
    {
      db::data data{ db::list{} };
      it = storage_.insert (std::move (key), std::move (data));
    }
  else
    {
      it = opt_it.value ();
      if (!it->second.is<db::list> ())
	return e_wrong_type;
    }

  auto &ls = it->second.get<db::list> ();
  for (std::size_t i = 1; i < args_.size (); i++)
    ls.push_back (std::move (args_[i]));

  return integer (to_int64 (ls.size ()));
}

resp::data
processor::exec_lpop ()
{
  // LPOP key [count]

  // RETURN:
  // - nil if the key does not exist.
  // - bulk string: when called without the count argument,
  //                the value of the first element.
  // - array: when called with the count argument,
  //          a list of popped elements.

  if (args_.size () != 1 && args_.size () != 2)
    return e_wrong_num_args ("lpop");

  bool with_count = args_.size () == 2;
  std::int64_t count = 1;
  if (with_count)
    {
      if (!try_lexical_convert (args_[1], count))
	return e_bad_integer;
      if (count <= 0)
	return e_value_out_of_range_positive;
    }

  const auto &key = args_[0];
  auto opt_it = storage_.find (key);
  if (!opt_it.has_value ())
    return with_count ? null_array () : null_bulk_string ();

  auto it = opt_it.value ();
  auto &data = it->second;
  if (!data.is<db::list> ())
    return e_wrong_type;

  auto &ls = data.get<db::list> ();
  if (!with_count)
    {
      if (ls.empty ())
	{
	  storage_.erase (it);
	  return null_bulk_string ();
	}

      std::string out = std::move (ls.front ());
      ls.pop_front ();
      if (ls.empty ())
	storage_.erase (it);
      return bulk_string (std::move (out));
    }

  if (ls.empty ())
    {
      storage_.erase (it);
      return null_array ();
    }

  std::vector<resp::data> out;
  while (count > 0 && !ls.empty ())
    {
      std::string val = std::move (ls.front ());
      ls.pop_front ();
      out.push_back (bulk_string (std::move (val)));
      count--;
    }

  if (ls.empty ())
    storage_.erase (it);
  return array (std::move (out));
}

resp::data
processor::exec_rpop ()
{
  // RPOP key [count]

  // RETURN:
  // - nil if the key does not exist.
  // - bulk string: when called without the count argument,
  //                the value of the first element.
  // - array: when called with the count argument,
  //          a list of popped elements.

  if (args_.size () != 1 && args_.size () != 2)
    return e_wrong_num_args ("rpop");

  bool with_count = args_.size () == 2;
  std::int64_t count = 1;
  if (with_count)
    {
      if (!try_lexical_convert (args_[1], count))
	return e_bad_integer;
      if (count <= 0)
	return e_value_out_of_range_positive;
    }

  const auto &key = args_[0];
  auto opt_it = storage_.find (key);
  if (!opt_it.has_value ())
    return with_count ? null_array () : null_bulk_string ();

  auto it = opt_it.value ();
  auto &data = it->second;
  if (!data.is<db::list> ())
    return e_wrong_type;

  auto &ls = data.get<db::list> ();
  if (!with_count)
    {
      if (ls.empty ())
	{
	  storage_.erase (it);
	  return null_bulk_string ();
	}

      std::string out = std::move (ls.back ());
      ls.pop_back ();
      if (ls.empty ())
	storage_.erase (it);
      return bulk_string (std::move (out));
    }

  if (ls.empty ())
    {
      storage_.erase (it);
      return null_array ();
    }

  std::vector<resp::data> out;
  while (count > 0 && !ls.empty ())
    {
      std::string val = std::move (ls.back ());
      ls.pop_back ();
      out.push_back (bulk_string (std::move (val)));
      count--;
    }

  if (ls.empty ())
    storage_.erase (it);
  return array (std::move (out));
}

} // namespace mini_redis
