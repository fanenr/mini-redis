#include "storage.h"

namespace mini_redis
{
namespace storage
{

// resp::data
// data::to_resp () const
// {
//   switch (index ())
//     {
//     case data::index_of<raw> ():
//       {
// 	const auto &str = get<raw> ();
// 	return resp::data{ resp::bulk_string{ str } };
//       }
//       break;

//     case data::index_of<integer> ():
//       {
// 	const auto &num = get<integer> ();
// 	return resp::data{ resp::integer{ num } };
//       }
//       break;

//     case data::index_of<list> ():
//       {
// 	const auto &lst = get<list> ();

// 	std::vector<resp::data> vec;
// 	vec.reserve (lst.size ());
// 	for (const auto &i : lst)
// 	  vec.push_back (resp::data{ resp::bulk_string{ i } });

// 	return resp::data{ resp::array{ std::move (vec) } };
//       }
//       break;

//     case data::index_of<set> ():
//       {
// 	const auto &st = get<set> ();

// 	std::vector<resp::data> vec;
// 	vec.reserve (st.size ());
// 	for (const auto &i : st)
// 	  vec.push_back (resp::data{ resp::bulk_string{ i } });

// 	return resp::data{ resp::array{ std::move (vec) } };
//       }
//       break;

//     case data::index_of<hashtable> ():
//       {
// 	const auto &ht = get<hashtable> ();

// 	std::vector<resp::data> vec;
// 	vec.reserve (ht.size () * 2);
// 	for (const auto &p : ht)
// 	  {
// 	    vec.push_back (resp::data{ resp::bulk_string{ p.first } });
// 	    vec.push_back (resp::data{ resp::bulk_string{ p.second } });
// 	  }

// 	return resp::data{ resp::array{ std::move (vec) } };
//       }
//       break;

//     default:
//       BOOST_THROW_EXCEPTION (std::logic_error ("bad data"));
//     }
// }

} // namespace storage
} // namespace mini_redis
