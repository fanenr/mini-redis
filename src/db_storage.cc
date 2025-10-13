#include "db_storage.h"

namespace mini_redis
{
namespace db
{

auto
storage::insert (std::string key, data value) -> iterator
{
  auto pair = db_.insert_or_assign (std::move (key), std::move (value));
  return pair.first;
}

auto
storage::find (const std::string &key) -> iterator
{
  auto it = db_.find (key);
  if (it == db_.end ())
    return it;

  auto ttl_it = ttl_.find (key);
  if (ttl_it == ttl_.end ())
    return it;

  auto expires = ttl_it->second;
  auto now = clock_type::now ();
  if (now < expires)
    return it;

  ttl_.erase (ttl_it);
  db_.erase (it);
  return db_.end ();
}

void
storage::erase (iterator it)
{
  BOOST_ASSERT (it != db_.end ());
  const auto &key = it->first;

  ttl_.erase (key);
  db_.erase (it);
}

auto
storage::end () -> iterator
{
  return db_.end ();
}

void
storage::expire_after (iterator it, duration dur)
{
  BOOST_ASSERT (it != db_.end ());
  const auto &key = it->first;

  auto expires = clock_type::now () + dur;
  ttl_.insert_or_assign (key, expires);
}

void
storage::expire_at (iterator it, time_point at)
{
  BOOST_ASSERT (it != db_.end ());
  const auto &key = it->first;

  ttl_.insert_or_assign (key, at);
}

auto
storage::ttl (iterator it) -> optional<duration>
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

} // namespace db
} // namespace mini_redis
