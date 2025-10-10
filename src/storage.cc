#include "storage.h"

namespace mini_redis
{
namespace storage
{

resp::data
data::to_resp () const
{
  switch (index ())
    {
    case raw_index:
      {
	const auto &str = get<raw> ();
	return resp::data{ resp::bulk_string{ str } };
      }
      break;

    case integer_index:
      {
	const auto &num = get<integer> ();
	return resp::data{ resp::integer{ num } };
      }
      break;

    case list_index:
      {
	const auto &lst = get<list> ();

	std::vector<resp::data> vec;
	vec.reserve (lst.size ());
	for (const auto &i : lst)
	  vec.push_back (resp::data{ resp::bulk_string{ i } });

	return resp::data{ resp::array{ std::move (vec) } };
      }
      break;

    case set_index:
      {
	const auto &st = get<set> ();

	std::vector<resp::data> vec;
	vec.reserve (st.size ());
	for (const auto &i : st)
	  vec.push_back (resp::data{ resp::bulk_string{ i } });

	return resp::data{ resp::array{ std::move (vec) } };
      }
      break;

    case hashtable_index:
      {
	const auto &ht = get<hashtable> ();

	std::vector<resp::data> vec;
	vec.reserve (ht.size () * 2);
	for (const auto &p : ht)
	  {
	    vec.push_back (resp::data{ resp::bulk_string{ p.first } });
	    vec.push_back (resp::data{ resp::bulk_string{ p.second } });
	  }

	return resp::data{ resp::array{ std::move (vec) } };
      }
      break;

    default:
      BOOST_THROW_EXCEPTION (std::logic_error ("bad data"));
    }
}

} // namespace storage
} // namespace mini_redis
