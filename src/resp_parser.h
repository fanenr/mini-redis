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
  void append (string_view chunk);
  std::size_t parse ();
  data pop ();
  std::size_t size () const;
  bool empty () const;

private:
  bool try_parse ();
  void push_value (data resp);
  std::size_t find_crlf () const;
  std::size_t parse_simple_string (optional<data> &out);
  std::size_t parse_simple_error (optional<data> &out);
  std::size_t parse_bulk_string (optional<data> &out);
  std::size_t parse_integer (optional<data> &out);
  std::size_t parse_array (optional<data> &out);

private:
  struct frame
  {
    std::size_t expected;
    std::vector<data> array;
  };

  std::string buffer_;
  std::deque<data> results_;
  std::vector<frame> frames_;
}; // class parser

} // namespace resp
} // namespace mini_redis

#endif // RESP_PARSER_H
