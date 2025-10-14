#include "resp_parser.h"

namespace mini_redis
{
namespace resp
{

std::size_t
parser::size () const
{
  return results_.size ();
}

bool
parser::empty () const
{
  return results_.empty ();
}

data
parser::pop ()
{
  if (!empty ())
    {
      auto resp = std::move (results_.front ());
      results_.pop_front ();
      return resp;
    }
  BOOST_THROW_EXCEPTION (std::logic_error ("results is empty"));
}

void
parser::append (string_view chunk)
{
  buffer_.append (chunk.data (), chunk.size ());
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

bool
parser::try_parse ()
{
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
      // TODO: response error
      // bad resp: unknown prefix
      consumed = 1;
    }

  if (!consumed)
    return false;
  buffer_.erase (0, consumed);

  if (resp)
    push_value (std::move (*resp));
  return true;
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

  auto str = std::string{ buffer_.data () + 1, pos - 1 };
  out = data{ simple_string{ std::move (str) } };
  return pos + 2;
}

std::size_t
parser::parse_simple_error (optional<data> &out)
{
  auto pos = find_crlf ();
  if (pos == std::string::npos)
    return 0;

  auto str = std::string{ buffer_.data () + 1, pos - 1 };
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
      // TODO: response error
      // bad bulk string: missing digits
      return pos1 + 2;
    }

  std::int64_t len;
  if (!try_lexical_convert (buffer_.data () + 1, pos1 - 1, len))
    {
      // TODO: response error
      // bad bulk string: invalid length
      return pos1 + 2;
    }
  if (len == -1)
    {
      out = data{ bulk_string{ boost::none } };
      return pos1 + 2;
    }
  if (len < 0)
    {
      // TODO: response error
      // bad bulk string: invalid length
      return pos1 + 2;
    }

  // TODO: limit bulk string length

  auto pos2 = pos1 + 2 + len;
  if (buffer_.size () < pos2 + 2)
    return 0;
  if (buffer_[pos2] != '\r' || buffer_[pos2 + 1] != '\n')
    {
      // TODO: response error
      // bad bulk string: missing CRLF after data
      return pos2;
    }

  auto str = std::string{ buffer_.data () + pos1 + 2,
			  static_cast<std::size_t> (len) };
  out = data{ bulk_string{ std::move (str) } };
  return pos2 + 2;
}

std::size_t
parser::parse_integer (optional<data> &out)
{
  auto pos = find_crlf ();
  if (pos == std::string::npos)
    return 0;
  if (pos == 1)
    {
      // TODO: response error
      // bad integer: missing digits
      return pos + 2;
    }

  std::int64_t num;
  if (!try_lexical_convert (buffer_.data () + 1, pos - 1, num))
    {
      // TODO: response error
      // bad integer: invalid number
      return pos + 2;
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
      // TODO: response error
      // bad array: missing digits
      return pos + 2;
    }

  std::int64_t len;
  if (!try_lexical_convert (buffer_.data () + 1, pos - 1, len))
    {
      // TODO: response error
      // bad array: invalid length
      return pos + 2;
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
      // TODO: response error
      // bad array: invalid length
      return pos + 2;
    }

  frame frm{ static_cast<std::size_t> (len), {} };
  frames_.push_back (std::move (frm));
  return pos + 2;
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
	{
	  results_.push_back (std::move (resp));
	  break;
	}
      else
	frames_.back ().array.push_back (std::move (resp));
    }
}

} // namespace resp
} // namespace mini_redis
