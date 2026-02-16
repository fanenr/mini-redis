#ifndef DB_STORAGE_H
#define DB_STORAGE_H

#include "pch.h"

#include "db_data.h"

namespace mini_redis
{
namespace db
{

typedef system_clock clock_type;
typedef clock_type::duration duration;
typedef clock_type::time_point time_point;

struct snapshot
{
  struct entry
  {
    std::string key;
    data value;
    optional<time_point> expire_at;
  };
  std::vector<entry> entries;
};

class storage
{
public:
  typedef unordered_flat_map<std::string, data> db_type;
  typedef unordered_flat_map<std::string, clock_type::time_point> ttl_type;
  typedef db_type::iterator iterator;

public:
  optional<iterator> find (const std::string &key);
  iterator insert (std::string key, data value);
  void erase (iterator it);

  void expire_after (iterator it, duration dur);
  void expire_at (iterator it, time_point at);
  optional<duration> ttl (iterator it);
  void clear_expires (iterator it);

  snapshot create_snapshot ();
  void replace_with_snapshot (snapshot snap);

private:
  db_type db_;
  ttl_type ttl_;
}; // class storage

} // namespace db
} // namespace mini_redis

#endif // DB_STORAGE_H
