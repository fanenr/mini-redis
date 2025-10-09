#ifndef DETAIL_RESP_PARSER_H
#define DETAIL_RESP_PARSER_H

#include "predef.h"
#include "resp_data.h"

#include <deque>
#include <vector>

#include <boost/lexical_cast.hpp>

namespace mini_redis
{
namespace detail
{
namespace resp
{

class parser
{
public:
  explicit parser () = default;

  std::size_t
  size () const
  {
    return results_.size ();
  }

  bool
  empty () const
  {
    return results_.empty ();
  }

  data
  pop ()
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
  append (string_view chunk)
  {
    buffer_.append (chunk.data (), chunk.size ());
  }

  std::size_t
  parse ()
  {
    auto before = results_.size ();
    while (!buffer_.empty ())
      if (!try_parse ())
	break;
    return results_.size () - before;
  }

private:
  bool
  try_parse ()
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
	BOOST_THROW_EXCEPTION (std::runtime_error ("invalid RESP prefix"));
      }

    if (!consumed)
      return false;
    buffer_.erase (0, consumed);

    if (resp)
      push_value (std::move (*resp));
    return true;
  }

private:
  std::size_t
  find_crlf () const
  {
    return buffer_.find ("\r\n");
  }

  std::size_t
  parse_simple_string (optional<data> &out)
  {
    auto pos = find_crlf ();
    if (pos == std::string::npos)
      return 0;

    auto str = std::string{ buffer_.data () + 1, pos - 1 };
    out = data{ simple_string{ std::move (str) } };
    return pos + 2;
  }

  std::size_t
  parse_simple_error (optional<data> &out)
  {
    auto pos = find_crlf ();
    if (pos == std::string::npos)
      return 0;

    auto str = std::string{ buffer_.data () + 1, pos - 1 };
    out = data{ simple_error{ std::move (str) } };
    return pos + 2;
  }

  std::size_t
  parse_bulk_string (optional<data> &out)
  {
    auto pos1 = find_crlf ();
    if (pos1 == std::string::npos)
      return 0;
    if (pos1 == 1)
      BOOST_THROW_EXCEPTION (
	  std::runtime_error ("bad bulk string: missing CRLF after length"));

    auto len
	= boost::lexical_cast<std::int64_t> (buffer_.data () + 1, pos1 - 1);
    if (len == -1)
      {
	out = data{ bulk_string{ boost::none } };
	return pos1 + 2;
      }
    if (len < 0)
      BOOST_THROW_EXCEPTION (
	  std::runtime_error ("bad bulk string: invalid length"));

    auto pos2 = pos1 + 2 + len;
    if (buffer_.size () < pos2 + 2)
      return 0;
    if (buffer_[pos2] != '\r' || buffer_[pos2 + 1] != '\n')
      BOOST_THROW_EXCEPTION (
	  std::runtime_error ("bad bulk string: missing CRLF after data"));

    auto str = std::string{ buffer_.data () + pos1 + 2,
			    static_cast<std::size_t> (len) };
    out = data{ bulk_string{ std::move (str) } };
    return pos2 + 2;
  }

  std::size_t
  parse_integer (optional<data> &out)
  {
    auto pos = find_crlf ();
    if (pos == std::string::npos)
      return 0;
    if (pos == 1)
      BOOST_THROW_EXCEPTION (std::runtime_error ("bad integer: missing CRLF"));

    auto num
	= boost::lexical_cast<std::int64_t> (buffer_.data () + 1, pos - 1);
    out = data{ integer{ num } };
    return pos + 2;
  }

  std::size_t
  parse_array (optional<data> &out)
  {
    auto pos = find_crlf ();
    if (pos == std::string::npos)
      return 0;
    if (pos == 1)
      BOOST_THROW_EXCEPTION (std::runtime_error ("bad array: missing CRLF"));

    auto len
	= boost::lexical_cast<std::int64_t> (buffer_.data () + 1, pos - 1);
    if (len == 0)
      {
	out = data{ array{ array::value_type{} } };
	return pos + 2;
      }
    if (len == -1)
      {
	out = data{ array{ boost::none } };
	return pos + 2;
      }
    if (len < 0)
      BOOST_THROW_EXCEPTION (std::runtime_error ("bad array: invalid length"));

    frame frm{ static_cast<std::size_t> (len), array{ array::value_type{} } };
    frames_.push_back (std::move (frm));
    return pos + 2;
  }

  void
  push_value (data resp)
  {
    if (frames_.empty ())
      {
	results_.push_back (std::move (resp));
	return;
      }

    BOOST_ASSERT (frames_.back ().array->has_value ());
    frames_.back ().array ()->push_back (std::move (resp));

    while (!frames_.empty ())
      {
	frame &frm = frames_.back ();
	BOOST_ASSERT (frm.array->has_value ());

	if (frm.array ()->size () < frm.expected)
	  break;

	data resp{ array{ std::move (frm.array) } };
	frames_.pop_back ();

	if (frames_.empty ())
	  {
	    results_.push_back (std::move (resp));
	    break;
	  }
	else
	  {
	    BOOST_ASSERT (frames_.back ().array->has_value ());
	    frames_.back ().array ()->push_back (std::move (resp));
	  }
      }
  }

private:
  struct frame
  {
    std::size_t expected;
    array array;
  };

  std::deque<data> results_;
  std::vector<frame> frames_;
  std::string buffer_;
}; // class parser

} // namespace resp
} // namespace detail
} // namespace mini_redis

#endif // DETAIL_RESP_PARSER_H
