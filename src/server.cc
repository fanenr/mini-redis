#include "server.h"
#include "session.h"

namespace mini_redis
{

server::server (int port, config cfg)
    : config_ (std::move (cfg)),
      acceptor_ (ioc_, tcp::endpoint (tcp::v4 (), port)),
      signals_ (ioc_, SIGINT, SIGTERM),
      executor_ (ioc_.get_executor (), config_)
{
  wait_signals ();
}

server::~server () { stop (); }

void
server::start ()
{
  start_accept ();
}

void
server::stop ()
{
  ioc_.stop ();
}

void
server::run ()
{
  ioc_.run ();
}

void
server::wait_signals ()
{
  auto handle_wait{ [this] (const error_code &ec, int sig) {
    if (!ec && (sig == SIGINT || sig == SIGTERM))
      this->stop ();
  } };
  signals_.async_wait (handle_wait);
}

void
server::start_accept ()
{
  auto handle_accept{ [this] (const error_code &ec, tcp::socket sock) {
    if (ec)
      return this->stop ();

    session::make (std::move (sock), executor_)->start ();

    this->start_accept ();
  } };
  acceptor_.async_accept (handle_accept);
}

} // namespace mini_redis
