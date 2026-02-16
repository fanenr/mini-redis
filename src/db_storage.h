#ifndef DB_STORAGE_H
#define DB_STORAGE_H

#include "pch.h"

#include "db_data.h"

namespace mini_redis
{
namespace db
{

class storage
{
public:
  typedef system_clock clock_type;
  typedef clock_type::duration duration;
  typedef clock_type::time_point time_point;

  typedef unordered_flat_map<std::string, data> db_type;
  typedef unordered_flat_map<std::string, clock_type::time_point> ttl_type;

  typedef db_type::iterator iterator;

  struct snapshot_entry
  {
    std::string key;
    data value;
    optional<time_point> expire_at;
  };

public:
  optional<iterator> find (const std::string &key);
  iterator insert (std::string key, data value);
  void erase (iterator it);

  void expire_after (iterator it, duration dur);
  void expire_at (iterator it, time_point at);
  optional<duration> ttl (iterator it);
  void clear_expires (iterator it);

  std::vector<snapshot_entry> snapshot ();
  void replace_with_snapshot (std::vector<snapshot_entry> entries);

private:
  db_type db_;
  ttl_type ttl_;
}; // class storage

} // namespace db
} // namespace mini_redis

#endif // DB_STORAGE_H
