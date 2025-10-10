#include "resp_data.h"

namespace mini_redis
{
namespace resp
{

std::string
data::encode () const
{
  std::ostringstream oss;
  encode (oss, *this);
  return oss.str ();
}

void
data::encode (std::ostringstream &oss, const data &resp)
{
  switch (resp.index ())
    {
    case simple_string_index:
      {
	const auto &ss = resp.get<simple_string> ();
	oss << simple_string_first << ss << "\r\n";
      }
      break;

    case simple_error_index:
      {
	const auto &se = resp.get<simple_error> ();
	oss << simple_error_first << se << "\r\n";
      }
      break;

    case bulk_string_index:
      {
	const auto &bs = resp.get<bulk_string> ();
	oss << bulk_string_first;
	if (bs)
	  oss << bs->size () << "\r\n" << *bs << "\r\n";
	else
	  oss << -1 << "\r\n";
      }
      break;

    case integer_index:
      {
	const auto &num = resp.get<integer> ();
	oss << integer_first << num << "\r\n";
      }
      break;

    case array_index:
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

} // namespace resp
} // namespace mini_redis
