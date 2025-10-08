#ifndef DETAIL_SERVER_H
#define DETAIL_SERVER_H

#include "executor.h"
#include "predef.h"
#include "session.h"

namespace mini_redis
{
namespace detail
{

class server
{
public:
  server (int port, const config &cfg)
      : acceptor_ (ioc_, tcp::endpoint (tcp::v4 (), port)),
	signals_ (ioc_, SIGINT, SIGTERM), executor_ (ioc_.get_executor (), cfg)
  {
    wait_signals ();
  }

  ~server () { stop (); }

  void
  start ()
  {
    start_accept ();
  }

  void
  stop ()
  {
    ioc_.stop ();
  }

  void
  run ()
  {
    ioc_.run ();
  }

private:
  void
  wait_signals ()
  {
    auto handle_wait{ [this] (const error_code &ec, int sig) {
      if (!ec && (sig == SIGINT || sig == SIGTERM))
	this->stop ();
    } };
    signals_.async_wait (handle_wait);
  }

  void
  start_accept ()
  {
    auto handle_accept{ [this] (const error_code &ec, tcp::socket sock) {
      if (ec)
	return this->stop ();

      session::make (std::move (sock), executor_)->start ();

      this->start_accept ();
    } };
    acceptor_.async_accept (handle_accept);
  }

private:
  asio::io_context ioc_;
  tcp::acceptor acceptor_;
  asio::signal_set signals_;
  executor executor_;
}; // class server

} // namespace detail
} // namespace mini_redis

#endif // DETAIL_SERVER_H
