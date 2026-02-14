#ifndef SERVER_H
#define SERVER_H

#include "pch.h"

#include "config.h"
#include "manager.h"

namespace mini_redis
{

class server
{
public:
  server (int port = 6379, config cfg = {});
  ~server ();

  server (const server &) = delete;
  server &operator= (const server &) = delete;
  server (server &&) noexcept = delete;
  server &operator= (server &&) noexcept = delete;

  void start ();
  void stop ();
  void run ();

private:
  void wait_signals ();
  void start_accept ();

private:
  asio::io_context ioc_;
  tcp::acceptor acceptor_;
  asio::signal_set signals_;
  manager manager_;
}; // class server

} // namespace mini_redis

#endif // SERVER_H
