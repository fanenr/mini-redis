#include "server.h"

int
main ()
{
  mini_redis::server srv;
  srv.start ();
  srv.run ();
}
