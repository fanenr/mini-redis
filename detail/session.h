#ifndef DETAIL_SESSION_H
#define DETAIL_SESSION_H

#include "executor.h"
#include "predef.h"
#include "resp_data.h"
#include "resp_parser.h"

#include <array>
#include <memory>

namespace mini_redis
{
namespace detail
{

class session : public std::enable_shared_from_this<session>
{
public:
  typedef std::shared_ptr<session> pointer;

  static pointer
  make (tcp::socket sock, executor &ex)
  {
    return pointer (new session (std::move (sock), ex));
  }

  void
  start ()
  {
    start_recv ();
  }

private:
  session (tcp::socket sock, executor &ex)
      : socket_ (std::move (sock)), executor_ (ex)
  {
  }

  void
  start_recv ()
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
  process ()
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
	  self->results_.push_back (result.encode ());
	} };
	executor_.post (std::move (cmd), cb);
      }

    auto task{ [self] () { self->start_send (); } };
    executor_.post (task);
  }

  void
  start_send ()
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

private:
  tcp::socket socket_;
  resp::parser parser_;
  std::array<char, 4096> buffer_;
  std::vector<std::string> results_;
  executor &executor_;
}; // class session

} // namespace detail
} // namespace mini_redis

#endif // DETAIL_SESSION_H
