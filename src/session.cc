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
    : state_{ normal }, socket_{ std::move (sock) },
      strand_{ socket_.get_executor () },
      idle_timeout_{ get_conn_idle_timeout (mgr.get_config ()) },
      idle_timer_{ strand_ }, manager_{ mgr },
      parser_{ make_parser_config (mgr.get_config ()) }
{
}

void
session::start ()
{
  auto self = shared_from_this ();
  auto start_cb = [self] ()
    {
      BOOST_ASSERT (self->strand_.running_in_this_thread ());
      self->refresh_idle_timeout ();
      self->start_recv ();
    };
  asio::dispatch (strand_, start_cb);
}

void
session::refresh_idle_timeout ()
{
  BOOST_ASSERT (strand_.running_in_this_thread ());
  if (state_ == closed)
    return;

  if (idle_timeout_ != milliseconds::zero ())
    {
      ++idle_timer_gen_;
      auto gen = idle_timer_gen_;
      idle_timer_.expires_after (idle_timeout_);

      auto self = shared_from_this ();
      auto wait_cb = [self, gen] (const error_code &ec)
	{
	  BOOST_ASSERT (self->strand_.running_in_this_thread ());
	  if (!ec && gen == self->idle_timer_gen_)
	    self->close ();
	};
      idle_timer_.async_wait (asio::bind_executor (strand_, wait_cb));
    }
}

void
session::start_recv ()
{
  BOOST_ASSERT (strand_.running_in_this_thread ());
  if (state_ == closed)
    return;

  auto self = shared_from_this ();
  auto receive_cb = [self] (const error_code &ec, std::size_t n)
    {
      BOOST_ASSERT (self->strand_.running_in_this_thread ());
      if (ec)
	return self->close ();

      self->refresh_idle_timeout ();
      self->parser_.append ({ self->recv_buffer_.data (), n });
      self->parser_.parse ();
      self->process ();
    };
  socket_.async_receive (asio::buffer (recv_buffer_),
			 asio::bind_executor (strand_, receive_cb));
}

void
session::process ()
{
  BOOST_ASSERT (strand_.running_in_this_thread ());

  if (!parser_.has_data ())
    {
      if (parser_.has_error ())
	{
	  results_.clear ();
	  results_.push_back (resp::simple_error{ parser_.pop_error () });
	  state_ = close_after_send;
	  return start_send ();
	}
      return start_recv ();
    }

  auto requests = std::make_shared<std::vector<resp::data>> ();
  requests->reserve (parser_.available_data ());
  while (parser_.has_data ())
    requests->push_back (parser_.pop_data ());

  auto parse_error = std::make_shared<optional<std::string>> ();
  if (parser_.has_error ())
    *parse_error = parser_.pop_error ();

  auto self = shared_from_this ();
  auto task = [self, requests, parse_error] (processor *pro)
    {
      auto responses = std::make_shared<std::vector<resp::data>> ();
      responses->reserve (requests->size ()
			  + (parse_error->has_value () ? 1 : 0));
      for (auto &i : *requests)
	responses->push_back (pro->execute (std::move (i)));

      bool should_close = false;
      if (parse_error->has_value ())
	{
	  responses->push_back (
	      resp::simple_error{ std::move (parse_error->value ()) });
	  should_close = true;
	}

      auto send_task = [self, responses, should_close] ()
	{
	  BOOST_ASSERT (self->strand_.running_in_this_thread ());
	  if (self->state_ == closed)
	    return;

	  self->results_.swap (*responses);
	  if (should_close)
	    self->state_ = close_after_send;
	  self->start_send ();
	};
      asio::post (self->strand_, send_task);
    };
  manager_.post (task);
}

void
session::start_send ()
{
  BOOST_ASSERT (strand_.running_in_this_thread ());
  if (state_ == closed)
    return;

  send_buffers_.clear ();
  send_buffers_.reserve (results_.size ());
  for (const auto &i : results_)
    send_buffers_.push_back (i.encode ());

  std::vector<asio::const_buffer> bufs;
  bufs.reserve (send_buffers_.size ());
  for (const auto &i : send_buffers_)
    bufs.push_back (asio::buffer (i));

  auto self = shared_from_this ();
  auto write_cb = [self] (const error_code &ec, std::size_t)
    {
      BOOST_ASSERT (self->strand_.running_in_this_thread ());
      if (ec)
	return self->close ();
      if (self->state_ == close_after_send)
	return self->close ();

      self->refresh_idle_timeout ();
      self->start_recv ();
    };
  asio::async_write (socket_, bufs, asio::bind_executor (strand_, write_cb));
}

void
session::close ()
{
  auto self = shared_from_this ();
  auto task = [self] ()
    {
      BOOST_ASSERT (self->strand_.running_in_this_thread ());
      if (self->state_ != closed)
	{
	  self->idle_timer_.cancel ();
	  error_code ec;
	  auto r = self->socket_.close (ec);
	  (void) r;
	  self->state_ = closed;
	}
    };
  asio::dispatch (strand_, task);
}

} // namespace mini_redis
