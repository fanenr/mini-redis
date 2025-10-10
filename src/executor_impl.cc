#include "executor_impl.h"
#include "resp_util.h"

namespace mini_redis
{

executor_impl::executor_impl (config &cfg)
    : config_ (cfg), cmds_{
	{ "SET", &executor_impl::exec_set },
	{ "GET", &executor_impl::exec_get },
	{ "PING", &executor_impl::exec_ping },
      }
{
}

resp::data
executor_impl::execute (resp::data resp)
{
  if (!resp.is<resp::array> ())
    return resp::error ("bad command: not an array");

  auto &arr = resp.get<resp::array> ();
  if (!arr.has_value ())
    return resp::error ("bad command: null array");

  auto &vec = arr.value ();
  if (vec.empty ())
    return resp::error ("bad command: empty array");

  auto validator{ [] (const resp::data &resp) {
    auto p = resp.get_if<resp::bulk_string> ();
    return p && p->has_value ();
  } };
  if (!std::all_of (vec.begin (), vec.end (), validator))
    return resp::error ("bad command: invalid arguments");

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
  return resp::error ("bad command: unknown command");
}

resp::data
executor_impl::exec_set ()
{
  if (args_.size () != 2)
    return resp::error ("bad command: argument count mismatch");

  auto &s0 = args_[0].get ();
  auto &s1 = args_[1].get ();
  storage::data data{ storage::raw{ std::move (s1) } };
  db_.insert_or_assign (std::move (s0), std::move (data));

  return resp::message ("OK");
}

resp::data
executor_impl::exec_get ()
{
  if (args_.size () != 1)
    return resp::error ("bad command: argument count mismatch");

  auto &s0 = args_[0].get ();
  auto it = db_.find (s0);

  if (it != db_.end ())
    return it->second.to_resp ();
  else
    return resp::null_string ();
}

resp::data
executor_impl::exec_ping ()
{
  switch (args_.size ())
    {
    case 0:
      return resp::message ("PONG");
    case 1:
      {
	auto &s0 = args_[0].get ();
	return resp::data{ resp::bulk_string{ std::move (s0) } };
      }
    default:
      return resp::error ("bad command: argument count mismatch");
    }
}

} // namespace mini_redis
