#include "session.h"

namespace mini_redis
{

namespace
{

resp::parser::config
make_parser_config (const config &cfg)
{
  resp::parser::config c;
  c.max_nesting = cfg.proto_max_nesting;
  c.max_bulk_len = cfg.proto_max_bulk_len;
  c.max_array_len = cfg.proto_max_array_len;
  c.max_inline_len = cfg.proto_max_inline_len;
  return c;
}

milliseconds
get_conn_idle_timeout (const config &cfg)
{
  auto ms = cfg.conn_idle_timeout_ms;
  if (ms == 0)
    return milliseconds::zero ();

  auto max = static_cast<std::size_t> (
      std::numeric_limits<milliseconds::rep>::max ());
  if (ms > max)
    ms = max;
  return milliseconds{ ms };
}

} // namespace

session::pointer
session::make (tcp::socket sock, manager &mgr)
{
  return std::make_shared<session> (std::move (sock), mgr);
}

session::session (tcp::socket sock, manager &mgr)
    : state_{ normal }, socket_{ std::move (sock) }, idle_timer_gen_{ 0 },
      idle_timer_{ socket_.get_executor () }, manager_{ mgr },
      parser_{ make_parser_config (mgr.get_config ()) }
{
}

void
session::start ()
{
  refresh_idle_timeout ();
  start_recv ();
}

void
session::refresh_idle_timeout ()
{
  auto timeout = get_conn_idle_timeout (manager_.get_config ());
  if (timeout == milliseconds::zero ())
    return;

  ++idle_timer_gen_;
  auto gen = idle_timer_gen_;

  idle_timer_.expires_after (timeout);

  auto self = shared_from_this ();
  auto wait_cb = [self, gen] (const error_code &ec)
    {
      if (!ec && gen == self->idle_timer_gen_)
	self->close ();
    };
  idle_timer_.async_wait (wait_cb);
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
      self->refresh_idle_timeout ();
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
      if (parser_.has_protocol_error ())
	{
	  std::string msg;
	  if (!parser_.take_protocol_error (msg))
	    msg = "ERR Protocol error: invalid request";

	  results_.clear ();
	  results_.push_back (resp::simple_error{ std::move (msg) });
	  state_ = close_after_send;
	  start_send ();
	  return;
	}

      start_recv ();
      return;
    }

  results_.clear ();
  results_.reserve (parser_.available_data () + 1);

  std::string err_msg;
  bool has_err = parser_.take_protocol_error (err_msg);

  auto self = shared_from_this ();
  auto task = [self, has_err, err_msg] (processor *pro)
    {
      while (self->parser_.has_data ())
	{
	  auto cmd = self->parser_.pop ();
	  auto res = pro->execute (std::move (cmd));
	  self->results_.push_back (std::move (res));
	}

      if (has_err)
	self->results_.push_back (resp::simple_error{ std::move (err_msg) });

      auto start_send = [self, has_err] ()
	{
	  if (has_err)
	    self->state_ = close_after_send;
	  self->start_send ();
	};
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

      if (self->state_ == close_after_send)
	{
	  self->close ();
	  return;
	}

      self->refresh_idle_timeout ();
      self->start_recv ();
    };
  asio::async_write (socket_, bufs, write_cb);
}

void
session::close ()
{
  idle_timer_.cancel ();
  error_code ec;
  auto r = socket_.close (ec);
  (void) r;
}

} // namespace mini_redis
