#include "resp_parser.h"

namespace mini_redis
{
namespace resp
{

parser::parser (config cfg) : config_{ std::move (cfg) } {}

void
parser::append_chunk (string_view chk)
{
  buffer_.append (chk.data (), chk.size ());
}

std::size_t
parser::parse ()
{
  auto before = results_.size ();
  while (!buffer_.empty ())
    if (!try_parse ())
      break;
  return results_.size () - before;
}

data
parser::pop ()
{
  auto resp = std::move (results_.front ());
  results_.pop_front ();
  return resp;
}

std::size_t
parser::available_data () const
{
  return results_.size ();
}

bool
parser::has_data () const
{
  return !results_.empty ();
}

bool
parser::has_protocol_error () const
{
  return protocol_error_;
}

bool
parser::take_protocol_error (std::string &out)
{
  if (!protocol_error_)
    return false;

  out = std::move (protocol_error_msg_);
  protocol_error_msg_.clear ();
  protocol_error_ = false;
  return true;
}

bool
parser::try_parse ()
{
  if (protocol_error_)
    return false;
  if (config_.max_inline_len != 0)
    {
      auto pos = find_crlf ();
      if (pos == std::string::npos && buffer_.size () > config_.max_inline_len)
	{
	  protocol_error ("ERR Protocol error: inline length exceeds "
			  "proto_max_inline_len");
	  return false;
	}
    }

  optional<data> resp;
  std::size_t consumed = 0;

  switch (buffer_[0])
    {
    case simple_string_first:
      consumed = parse_simple_string (resp);
      break;

    case simple_error_first:
      consumed = parse_simple_error (resp);
      break;

    case bulk_string_first:
      consumed = parse_bulk_string (resp);
      break;

    case integer_first:
      consumed = parse_integer (resp);
      break;

    case array_first:
      consumed = parse_array (resp);
      break;

    default:
      protocol_error ("ERR Protocol error: unknown prefix");
      return false;
    }

  if (consumed == 0)
    return false;
  else
    buffer_.erase (0, consumed);

  if (resp.has_value ())
    push_value (std::move (resp.value ()));
  return true;
}

void
parser::push_value (data resp)
{
  if (frames_.empty ())
    {
      results_.push_back (std::move (resp));
      return;
    }

  frames_.back ().array.push_back (std::move (resp));

  while (!frames_.empty ())
    {
      frame &frm = frames_.back ();

      if (frm.array.size () < frm.expected)
	break;

      data resp{ array{ std::move (frm.array) } };
      frames_.pop_back ();

      if (frames_.empty ())
	results_.push_back (std::move (resp));
      else
	frames_.back ().array.push_back (std::move (resp));
    }
}

void
parser::protocol_error (string_view msg)
{
  protocol_error_ = true;
  protocol_error_msg_.assign (msg.data (), msg.size ());
  frames_.clear ();
  buffer_.clear ();
}

std::size_t
parser::find_crlf () const
{
  return buffer_.find ("\r\n");
}

std::size_t
parser::parse_simple_string (optional<data> &out)
{
  auto pos = find_crlf ();
  if (pos == std::string::npos)
    return 0;

  std::string str{ buffer_.data () + 1, pos - 1 };
  out = data{ simple_string{ std::move (str) } };
  return pos + 2;
}

std::size_t
parser::parse_simple_error (optional<data> &out)
{
  auto pos = find_crlf ();
  if (pos == std::string::npos)
    return 0;

  std::string str{ buffer_.data () + 1, pos - 1 };
  out = data{ simple_error{ std::move (str) } };
  return pos + 2;
}

std::size_t
parser::parse_bulk_string (optional<data> &out)
{
  auto pos1 = find_crlf ();
  if (pos1 == std::string::npos)
    return 0;
  if (pos1 == 1)
    {
      protocol_error ("ERR Protocol error: missing bulk length");
      return 0;
    }

  std::int64_t len;
  if (!try_lexical_convert (buffer_.data () + 1, pos1 - 1, len))
    {
      protocol_error ("ERR Protocol error: invalid bulk length");
      return 0;
    }
  if (len == -1)
    {
      out = data{ bulk_string{ boost::none } };
      return pos1 + 2;
    }
  if (len < 0)
    {
      protocol_error ("ERR Protocol error: invalid bulk length");
      return 0;
    }

  auto ulen = static_cast<std::uint64_t> (len);
  if (config_.max_bulk_len != 0 && ulen > config_.max_bulk_len)
    {
      protocol_error (
	  "ERR Protocol error: bulk length exceeds proto_max_bulk_len");
      return 0;
    }
  if (ulen > std::numeric_limits<std::size_t>::max ())
    {
      protocol_error ("ERR Protocol error: bulk length is too large");
      return 0;
    }
  auto len_sz = static_cast<std::size_t> (ulen);

  auto data_start = pos1 + 2;
  if (buffer_.size () - data_start < len_sz)
    return 0;
  auto data_end = data_start + len_sz;
  if (buffer_.size () - data_end < 2)
    return 0;
  if (buffer_[data_end] != '\r' || buffer_[data_end + 1] != '\n')
    {
      protocol_error ("ERR Protocol error: bad bulk string encoding");
      return 0;
    }

  std::string str{ buffer_.data () + data_start, len_sz };
  out = data{ bulk_string{ std::move (str) } };
  return data_end + 2;
}

std::size_t
parser::parse_integer (optional<data> &out)
{
  auto pos = find_crlf ();
  if (pos == std::string::npos)
    return 0;
  if (pos == 1)
    {
      protocol_error ("ERR Protocol error: missing integer");
      return 0;
    }

  std::int64_t num;
  if (!try_lexical_convert (buffer_.data () + 1, pos - 1, num))
    {
      protocol_error ("ERR Protocol error: invalid integer");
      return 0;
    }

  out = data{ integer{ num } };
  return pos + 2;
}

std::size_t
parser::parse_array (optional<data> &out)
{
  auto pos = find_crlf ();
  if (pos == std::string::npos)
    return 0;
  if (pos == 1)
    {
      protocol_error ("ERR Protocol error: missing array length");
      return 0;
    }

  std::int64_t len;
  if (!try_lexical_convert (buffer_.data () + 1, pos - 1, len))
    {
      protocol_error ("ERR Protocol error: invalid array length");
      return 0;
    }
  if (len == 0)
    {
      out = data{ array{ std::vector<data>{} } };
      return pos + 2;
    }
  if (len == -1)
    {
      out = data{ array{ boost::none } };
      return pos + 2;
    }
  if (len < 0)
    {
      protocol_error ("ERR Protocol error: invalid array length");
      return 0;
    }

  auto ulen = static_cast<std::uint64_t> (len);
  if (config_.max_array_len != 0 && ulen > config_.max_array_len)
    {
      protocol_error (
	  "ERR Protocol error: array length exceeds proto_max_array_len");
      return 0;
    }
  if (ulen > std::numeric_limits<std::size_t>::max ())
    {
      protocol_error ("ERR Protocol error: array length is too large");
      return 0;
    }
  if (config_.max_nesting != 0 && frames_.size () + 1 > config_.max_nesting)
    {
      protocol_error (
	  "ERR Protocol error: array nesting exceeds proto_max_nesting");
      return 0;
    }

  frame frm{ static_cast<std::size_t> (ulen), {} };
  frames_.push_back (std::move (frm));
  return pos + 2;
}

} // namespace resp
} // namespace mini_redis
