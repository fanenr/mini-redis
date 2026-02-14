#ifndef CONFIG_H
#define CONFIG_H

#include "pch.h"

namespace mini_redis
{

struct config
{
  // 0 means no limit
  std::size_t timeout = 0;
  std::size_t proto_max_bulk_len = 512 * 1024 * 1024;
  std::size_t proto_max_array_len = 1024 * 1024;
  std::size_t proto_max_nesting = 128;
  std::size_t proto_max_inline_len = 64 * 1024;
}; // class config

} // namespace mini_redis

#endif // CONFIG_H
