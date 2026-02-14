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
  enum
  {
    normal = 0,
    close_after_send = 1,
  };

  typedef std::shared_ptr<session> pointer;

  static pointer make (tcp::socket sock, manager &mgr);
  session (tcp::socket sock, manager &mgr);

  void start ();

private:
  void start_recv ();
  void process ();
  void start_send ();
  void close ();

private:
  int state_;
  tcp::socket socket_;
  std::vector<resp::data> results_;
  std::array<char, 4096> recv_buffer_;
  std::vector<std::string> send_buffers_;

  manager &manager_;
  resp::parser parser_;
}; // class session

} // namespace mini_redis

#endif // SESSION_H
