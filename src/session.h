#ifndef SESSION_H
#define SESSION_H

#include "executor.h"
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

  static pointer make (tcp::socket sock, executor &ex);
  session (tcp::socket sock, executor &ex);

  void start ();

private:
  void start_recv ();
  void process ();
  void start_send ();

private:
  tcp::socket socket_;
  std::array<char, 4096> buffer_;
  std::vector<std::string> results_;

  resp::parser parser_;
  executor &executor_;
}; // class session

} // namespace mini_redis

#endif // SESSION_H
