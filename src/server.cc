#include "server.h"
#include "session.h"

namespace mini_redis
{

server::server (std::uint16_t port, config cfg)
    : acceptor_{ ioc_, tcp::endpoint{ tcp::v4 (), port } },
      signals_{ ioc_, SIGINT, SIGTERM },
      manager_{ ioc_.get_executor (), std::move (cfg) }
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
  auto wait_cb = [this] (const error_code &ec, int sig)
    {
      if (!ec && (sig == SIGINT || sig == SIGTERM))
	this->stop ();
    };
  signals_.async_wait (wait_cb);
}

void
server::start_accept ()
{
  auto accept_cb = [this] (const error_code &ec, tcp::socket sock)
    {
      if (ec)
	return this->stop ();
      session::make (std::move (sock), manager_)->start ();
      start_accept ();
    };
  acceptor_.async_accept (accept_cb);
}

} // namespace mini_redis
