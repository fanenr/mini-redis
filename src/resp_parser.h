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
  void set_limits (std::size_t max_bulk_len, std::size_t max_array_len,
		   std::size_t max_nesting, std::size_t max_inline_len);
  void append_chunk (string_view chk);
  std::size_t parse ();
  data pop ();
  std::size_t available_data () const;
  bool has_data () const;
  bool has_protocol_error () const;
  bool take_protocol_error (std::string &out);

private:
  bool try_parse ();
  void push_value (data resp);
  void protocol_error (std::string msg);
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

  std::size_t max_bulk_len_ = 0;
  std::size_t max_array_len_ = 0;
  std::size_t max_nesting_ = 0;
  std::size_t max_inline_len_ = 0;

  bool protocol_error_ = false;
  std::string protocol_error_msg_;

  std::string buffer_;
  std::deque<data> results_;
  std::vector<frame> frames_;
}; // class parser

} // namespace resp
} // namespace mini_redis

#endif // RESP_PARSER_H
