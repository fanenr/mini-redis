#include "session.h"

namespace mini_redis
{

session::pointer
session::make (tcp::socket sock, manager &mgr)
{
  return std::make_shared<session> (std::move (sock), mgr);
}

session::session (tcp::socket sock, manager &mgr)
    : socket_ (std::move (sock)), manager_ (mgr)
{
}

void
session::start ()
{
  start_recv ();
}

void
session::start_recv ()
{
  auto self = shared_from_this ();
  auto handle_recv{ [self] (const error_code &ec, std::size_t n) {
    if (ec)
      self->socket_.close ();

    self->parser_.append ({ self->buffer_.data (), n });
    self->parser_.parse ();
    self->process ();
  } };
  socket_.async_receive (asio::buffer (buffer_), handle_recv);
}

void
session::process ()
{
  if (parser_.empty ())
    return start_recv ();

  results_.clear ();
  results_.reserve (parser_.size ());

  auto self = shared_from_this ();
  while (!parser_.empty ())
    {
      auto cmd = parser_.pop ();
      auto cb{ [self] (resp::data result) {
	self->results_.push_back (result.to_string ());
      } };
      manager_.post (std::move (cmd), cb);
    }

  auto task{ [self] () { self->start_send (); } };
  manager_.post (task);
}

void
session::start_send ()
{
  std::vector<asio::const_buffer> buffers;
  buffers.reserve (results_.size ());
  for (const auto &result : results_)
    buffers.push_back (asio::buffer (result));

  auto self = shared_from_this ();
  auto handle_write{ [self] (const error_code &ec, std::size_t) {
    if (ec)
      self->socket_.close ();

    self->start_recv ();
  } };
  asio::async_write (socket_, buffers, handle_write);
}

} // namespace mini_redis
