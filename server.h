#ifndef SERVER_H
#define SERVER_H

#include "config.h"
#include "detail/server.h"

namespace mini_redis
{

class server
{
public:
  explicit server (int port = 6379, const config &cfg = {})
      : impl_ (new detail::server (port, cfg))
  {
  }

  ~server () { delete impl_; }

  void
  start ()
  {
    impl_->start ();
  }

  void
  stop ()
  {
    impl_->stop ();
  }

  void
  run ()
  {
    impl_->run ();
  }

private:
  detail::server *impl_;
}; // class server

} // namespace mini_redis

#endif // SERVER_H
