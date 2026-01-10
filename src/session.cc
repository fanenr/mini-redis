#include "session.h"

namespace mini_redis
{

auto
session::make (tcp::socket sock, manager &mgr) -> session::pointer
{
  return std::make_shared<session> (std::move (sock), mgr);
}

session::session (tcp::socket sock, manager &mgr)
    : socket_{ std::move (sock) }, manager_{ mgr }
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
  auto receive_cb = [self] (const error_code &ec, std::size_t n)
    {
      if (ec)
	{
	  self->close ();
	  return;
	}
      self->parser_.append_chunk ({ self->recv_buffer_.data (), n });
      self->parser_.parse ();
      self->process ();
    };
  socket_.async_receive (asio::buffer (recv_buffer_), receive_cb);
}

void
session::process ()
{
  if (!parser_.has_data ())
    {
      start_recv ();
      return;
    }

  results_.clear ();
  results_.reserve (parser_.available_data ());

  auto self = shared_from_this ();
  auto task = [self] (processor *pro)
    {
      while (self->parser_.has_data ())
	{
	  auto cmd = self->parser_.pop ();
	  auto result = pro->execute (std::move (cmd));
	  self->results_.push_back (std::move (result));
	}
      auto start_send = [self] () { self->start_send (); };
      auto ex = self->socket_.get_executor ();
      asio::post (ex, start_send);
    };
  manager_.post (task);
}

void
session::start_send ()
{
  send_buffers_.clear ();
  send_buffers_.reserve (results_.size ());
  for (const auto &r : results_)
    send_buffers_.push_back (r.encode ());

  std::vector<asio::const_buffer> bufs;
  bufs.reserve (send_buffers_.size ());
  for (const auto &s : send_buffers_)
    bufs.push_back (asio::buffer (s));

  auto self = shared_from_this ();
  auto write_cb = [self] (const error_code &ec, std::size_t)
    {
      if (ec)
	{
	  self->close ();
	  return;
	}
      self->start_recv ();
    };
  asio::async_write (socket_, bufs, write_cb);
}

void
session::close ()
{
  socket_.close ();
}

} // namespace mini_redis
