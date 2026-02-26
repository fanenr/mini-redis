#ifndef SESSION_H
#define SESSION_H

#include "pch.h"

#include "manager.h"
#include "resp_data.h"
#include "resp_parser.h"

namespace mini_redis
{

class session : public std::enable_shared_from_this<session>
{
public:
  typedef std::shared_ptr<session> pointer;

  static pointer make (tcp::socket sock, manager &mgr);
  session (tcp::socket sock, manager &mgr);

  void start ();

private:
  void refresh_idle_timeout ();
  void start_recv ();
  void process ();
  void start_send ();
  void close ();

private:
  enum
  {
    normal,
    closed,
    close_after_send,
  };

  int state_;
  tcp::socket socket_;
  asio::strand<asio::any_io_executor> strand_;

  milliseconds idle_timeout_;
  std::uint64_t idle_timer_gen_;
  asio::steady_timer idle_timer_;

  std::vector<resp::data> results_;
  std::array<char, 4096> recv_buffer_;
  std::vector<std::string> send_buffers_;

  manager &manager_;
  resp::parser parser_;
}; // class session

} // namespace mini_redis

#endif // SESSION_H
