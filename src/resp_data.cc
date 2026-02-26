#include "resp_data.h"

static const char *CRLF = "\r\n";

namespace mini_redis
{
namespace resp
{

void
encode_impl (std::ostringstream &oss, const data &resp)
{
  switch (resp.index ())
    {
    case data::index_of<simple_string> ():
      {
	const auto &ss = resp.get<simple_string> ();
	oss << simple_string_first << ss << CRLF;
      }
      break;

    case data::index_of<simple_error> ():
      {
	const auto &se = resp.get<simple_error> ();
	oss << simple_error_first << se << CRLF;
      }
      break;

    case data::index_of<bulk_string> ():
      {
	const auto &bs = resp.get<bulk_string> ();
	oss << bulk_string_first;
	if (bs.has_value ())
	  oss << bs->size () << CRLF << bs.value () << CRLF;
	else
	  oss << -1 << CRLF;
      }
      break;

    case data::index_of<integer> ():
      {
	const auto &num = resp.get<integer> ();
	oss << integer_first << num << CRLF;
      }
      break;

    case data::index_of<array> ():
      {
	const auto &arr = resp.get<array> ();
	oss << array_first;
	if (arr.has_value ())
	  {
	    const auto &vec = arr.value ();
	    oss << vec.size () << CRLF;
	    for (const auto &i : vec)
	      encode_impl (oss, i);
	  }
	else
	  oss << -1 << CRLF;
      }
      break;

    default:
      BOOST_THROW_EXCEPTION (std::logic_error ("bad data"));
    }
}

std::string
data::encode () const
{
  std::ostringstream oss;
  encode_impl (oss, *this);
  return oss.str ();
}

} // namespace resp
} // namespace mini_redis
