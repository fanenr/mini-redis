#ifndef SESSION_H
#define SESSION_H

#include "manager.h"
#include "predef.h"
#include "resp_data.h"
#include "resp_parser.h"

#include <array>
#include <memory>

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
  void start_recv ();
  void process ();
  void start_send ();
  void close ();

private:
  tcp::socket socket_;
  std::vector<resp::data> results_;
  std::array<char, 4096> recv_buffer_;
  std::vector<std::string> send_buffers_;

  manager &manager_;
  resp::parser parser_;
}; // class session

} // namespace mini_redis

#endif // SESSION_H
