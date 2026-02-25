#include "src/server.h"

int
main (int argc, char **argv)
{
  std::uint16_t port = 6379;

  if (argc != 1)
    {
      if (argc != 3 || std::string{ argv[1] } != "--port")
	{
	  std::fprintf (stderr, "Usage: %s [--port <1-65535>]\n", argv[0]);
	  return 1;
	}

      std::int64_t n;
      if (!mini_redis::try_lexical_convert (argv[2], n) || n <= 0
	  || n > std::numeric_limits<std::uint16_t>::max ())
	{
	  std::fprintf (stderr, "Invalid port: %s\n", argv[2]);
	  return 1;
	}

      port = static_cast<std::uint16_t> (n);
    }

  mini_redis::server srv{ port };
  srv.start ();
  srv.run ();
}
