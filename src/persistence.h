#ifndef PERSISTENCE_H
#define PERSISTENCE_H

#include "pch.h"

#include "db_storage.h"

namespace mini_redis
{
namespace persistence
{

struct save_result
{
  bool ok = false;
  std::string err;
};

struct load_result
{
  bool ok = false;
  std::string err;
  std::vector<db::storage::snapshot_entry> entries;
};

save_result save_to (string_view path, db::storage &storage);
load_result load_from (string_view path);

} // namespace persistence
} // namespace mini_redis

#endif // PERSISTENCE_H
