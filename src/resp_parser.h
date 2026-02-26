#ifndef RESP_PARSER_H
#define RESP_PARSER_H

#include "pch.h"

#include "resp_data.h"

namespace mini_redis
{
namespace resp
{

class parser
{
public:
  struct config
  {
    // 0 means no limit
    std::size_t max_nesting = 0;
    std::size_t max_bulk_len = 0;
    std::size_t max_array_len = 0;
    std::size_t max_inline_len = 0;
  };

  explicit parser (config cfg);

  void append (string_view chk);
  std::size_t parse ();

  std::size_t available_data () const;
  bool has_data () const;
  data pop_data ();

  bool has_error () const;
  std::string pop_error ();

private:
  bool try_parse ();
  void push_value (data resp);
  void protocol_error (string_view msg);
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

  config config_;
  std::string buffer_;
  std::deque<data> results_;
  std::vector<frame> frames_;
  optional<std::string> error_;
}; // class parser

} // namespace resp
} // namespace mini_redis

#endif // RESP_PARSER_H
