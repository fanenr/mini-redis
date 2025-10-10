#ifndef RESP_PARSER_H
#define RESP_PARSER_H

#include "predef.h"
#include "resp_data.h"

#include <deque>
#include <vector>

namespace mini_redis
{
namespace resp
{

class parser
{
public:
  explicit parser () = default;

  std::size_t size () const;
  bool empty () const;
  data pop ();
  void append (string_view chunk);
  std::size_t parse ();

private:
  bool try_parse ();
  std::size_t find_crlf () const;
  std::size_t parse_simple_string (optional<data> &out);
  std::size_t parse_simple_error (optional<data> &out);
  std::size_t parse_bulk_string (optional<data> &out);
  std::size_t parse_integer (optional<data> &out);
  std::size_t parse_array (optional<data> &out);
  void push_value (data resp);

private:
  struct frame
  {
    std::size_t expected;
    resp::array array;
  };

  std::deque<data> results_;
  std::vector<frame> frames_;
  std::string buffer_;
}; // class parser

} // namespace resp
} // namespace mini_redis

#endif // RESP_PARSER_H
