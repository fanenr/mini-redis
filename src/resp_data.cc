#include "resp_data.h"

namespace mini_redis
{
namespace resp
{

void
encode (std::ostringstream &oss, const data &resp)
{
  switch (resp.index ())
    {
    case data::index_of<simple_string> ():
      {
	const auto &ss = resp.get<simple_string> ();
	oss << simple_string_first << ss << "\r\n";
      }
      break;

    case data::index_of<simple_error> ():
      {
	const auto &se = resp.get<simple_error> ();
	oss << simple_error_first << se << "\r\n";
      }
      break;

    case data::index_of<bulk_string> ():
      {
	const auto &bs = resp.get<bulk_string> ();
	oss << bulk_string_first;
	if (bs)
	  oss << bs->size () << "\r\n" << *bs << "\r\n";
	else
	  oss << -1 << "\r\n";
      }
      break;

    case data::index_of<integer> ():
      {
	const auto &num = resp.get<integer> ();
	oss << integer_first << num << "\r\n";
      }
      break;

    case data::index_of<array> ():
      {
	const auto &arr = resp.get<array> ();
	oss << array_first;
	if (arr)
	  {
	    const auto &vec = arr.value ();
	    oss << vec.size () << "\r\n";
	    for (const auto &i : vec)
	      encode (oss, i);
	  }
	else
	  oss << -1 << "\r\n";
      }
      break;

    default:
      BOOST_THROW_EXCEPTION (std::logic_error ("bad data"));
    }
}

std::string
data::to_string () const
{
  std::ostringstream oss;
  encode (oss, *this);
  return oss.str ();
}

} // namespace resp
} // namespace mini_redis
