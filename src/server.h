#ifndef SERVER_H
#define SERVER_H

#include "config.h"
#include "executor.h"
#include "predef.h"

namespace mini_redis
{

class server
{
public:
  server (int port = 6379, config cfg = {});
  ~server ();

  void start ();
  void stop ();
  void run ();

private:
  void wait_signals ();
  void start_accept ();

private:
  config config_;

  asio::io_context ioc_;
  tcp::acceptor acceptor_;
  asio::signal_set signals_;

  executor executor_;
}; // class server

} // namespace mini_redis

#endif // SERVER_H
