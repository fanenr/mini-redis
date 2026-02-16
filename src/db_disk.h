#ifndef DB_DISK_H
#define DB_DISK_H

#include "pch.h"

#include "db_storage.h"

namespace mini_redis
{
namespace db
{

result<void, std::string> save_to (string_view path, snapshot snap);
result<void, std::string> load_from (string_view path, snapshot &out);

} // namespace db
} // namespace mini_redis

#endif // DB_DISK_H
