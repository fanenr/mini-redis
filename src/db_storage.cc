#include "db_storage.h"

namespace mini_redis
{
namespace db
{

optional<storage::iterator>
storage::find (const std::string &key)
{
  auto it = db_.find (key);
  if (it == db_.end ())
    return boost::none;

  auto ttl_it = ttl_.find (key);
  if (ttl_it == ttl_.end ())
    return it;

  auto expires = ttl_it->second;
  auto now = clock_type::now ();
  if (now < expires)
    return it;

  ttl_.erase (ttl_it);
  db_.erase (it);
  return boost::none;
}

storage::iterator
storage::insert (std::string key, data value)
{
  auto pair = db_.insert_or_assign (std::move (key), std::move (value));
  return pair.first;
}

void
storage::erase (iterator it)
{
  BOOST_ASSERT (it != db_.end ());

  const auto &key = it->first;
  ttl_.erase (key);
  db_.erase (it);
}

void
storage::expire_after (iterator it, duration dur)
{
  auto expires = clock_type::now () + dur;
  expire_at (it, expires);
}

void
storage::expire_at (iterator it, time_point at)
{
  BOOST_ASSERT (it != db_.end ());

  const auto &key = it->first;
  ttl_.insert_or_assign (key, at);
}

optional<duration>
storage::ttl (iterator it)
{
  BOOST_ASSERT (it != db_.end ());

  const auto &key = it->first;
  auto ttl_it = ttl_.find (key);
  if (ttl_it == ttl_.end ())
    return boost::none;

  auto expires = ttl_it->second;
  auto now = clock_type::now ();
  return expires - now;
}

void
storage::clear_expires (iterator it)
{
  BOOST_ASSERT (it != db_.end ());

  const auto &key = it->first;
  ttl_.erase (key);
}

snapshot
storage::create_snapshot ()
{
  auto now = clock_type::now ();

  std::vector<std::string> expired_keys;
  expired_keys.reserve (ttl_.size ());
  snapshot out;
  out.entries.reserve (db_.size ());
  for (const auto &p : db_)
    {
      const auto &key = p.first;
      auto ttl_it = ttl_.find (key);
      if (ttl_it == ttl_.end ())
	{
	  out.entries.push_back ({ key, p.second, boost::none });
	  continue;
	}

      auto expires = ttl_it->second;
      if (now >= expires)
	expired_keys.push_back (key);
      else
	out.entries.push_back ({ key, p.second, expires });
    }

  for (const auto &key : expired_keys)
    {
      ttl_.erase (key);
      db_.erase (key);
    }

  return out;
}

void
storage::replace_with_snapshot (snapshot snap)
{
  db_type new_db;
  new_db.reserve (snap.entries.size ());
  ttl_type new_ttl;
  new_ttl.reserve (snap.entries.size ());
  for (auto &e : snap.entries)
    {
      auto pair
	  = new_db.insert_or_assign (std::move (e.key), std::move (e.value));
      const auto &key = pair.first->first;
      if (e.expire_at.has_value ())
	new_ttl.insert_or_assign (key, e.expire_at.value ());
      else
	new_ttl.erase (key);
    }

  db_.swap (new_db);
  ttl_.swap (new_ttl);
}

} // namespace db
} // namespace mini_redis
