#include "db_disk.h"

#include "resp_parser.h"

namespace mini_redis
{
namespace db
{

namespace
{

enum : std::int64_t
{
  type_string = data::index_of<string> (),
  type_integer = data::index_of<integer> (),
  type_list = data::index_of<list> (),
  type_set = data::index_of<set> (),
  type_hash = data::index_of<hashtable> (),
};

typedef result<void, std::string> result_type;

static const char format_magic[4]{ 'M', 'R', 'D', 'B' };
static const char format_version = 1;

void
append_array_header (std::string &out, std::size_t n)
{
  out.push_back (resp::array_first);
  out += std::to_string (n);
  out.append ("\r\n", 2);
}

void
append_bulk_string (std::string &out, string_view s)
{
  out.push_back (resp::bulk_string_first);
  out += std::to_string (s.size ());
  out.append ("\r\n", 2);
  out.append (s.data (), s.size ());
  out.append ("\r\n", 2);
}

void
append_integer (std::string &out, std::int64_t i)
{
  out.push_back (resp::integer_first);
  out += std::to_string (i);
  out.append ("\r\n", 2);
}

void
append_entry (std::string &out, const snapshot::entry &entry)
{
  append_array_header (out, 5);

  append_bulk_string (out, entry.key);
  const auto &value = entry.value;
  switch (value.index ())
    {
    case type_string:
      {
	append_integer (out, type_string);
	append_bulk_string (out, value.get<string> ());
      }
      break;

    case type_integer:
      {
	append_integer (out, type_integer);
	append_integer (out, value.get<integer> ());
      }
      break;

    case type_list:
      {
	append_integer (out, type_list);
	const auto &ls = value.get<list> ();
	append_array_header (out, ls.size ());
	for (const auto &i : ls)
	  append_bulk_string (out, i);
      }
      break;

    case type_set:
      {
	append_integer (out, type_set);
	const auto &st = value.get<set> ();
	append_array_header (out, st.size ());
	for (const auto &i : st)
	  append_bulk_string (out, i);
      }
      break;

    case type_hash:
      {
	append_integer (out, type_hash);
	const auto &ht = value.get<hashtable> ();
	append_array_header (out, ht.size () * 2);
	for (const auto &p : ht)
	  {
	    append_bulk_string (out, p.first);
	    append_bulk_string (out, p.second);
	  }
      }
      break;

    default:
      BOOST_THROW_EXCEPTION (std::logic_error ("bad data type"));
    }

  std::int64_t has_expire = 0;
  std::int64_t expire_at_ms = 0;
  if (entry.expire_at.has_value ())
    {
      has_expire = 1;
      expire_at_ms = duration_cast<milliseconds> (
			 entry.expire_at.value ().time_since_epoch ())
			 .count ();
    }

  append_integer (out, has_expire);
  append_integer (out, expire_at_ms);
}

std::string
format_errno (string_view prefix)
{
  int code = errno;
  std::string msg = prefix.to_string ();
  msg += ": ";
  msg += std::strerror (code);
  return msg;
}

bool
write_all (std::FILE *fp, string_view bytes)
{
  auto buf = bytes.data ();
  auto len = bytes.size ();

  while (len != 0)
    {
      auto n = std::fwrite (buf, 1, len, fp);
      if (n == 0)
	return false;

      buf += n;
      len -= n;
    }

  return true;
}

result_type
save_file (std::string path, string_view body)
{
  auto temp_path = path + ".tmp";
  auto backup_path = path + ".bak";
  const char *path_cstr = path.c_str ();
  const char *temp_path_cstr = temp_path.c_str ();
  const char *backup_path_cstr = backup_path.c_str ();

  std::remove (temp_path_cstr);

  auto *fp = std::fopen (temp_path_cstr, "wb");
  const char header[5]{ format_magic[0], format_magic[1], format_magic[2],
			format_magic[3], static_cast<char> (format_version) };
  if (fp == nullptr)
    return format_errno ("save failed: cannot open temporary file");
  if (!write_all (fp, { header, sizeof (header) }))
    {
      std::fclose (fp);
      std::remove (temp_path_cstr);
      return format_errno ("save failed: cannot write header");
    }
  if (!body.empty () && !write_all (fp, body))
    {
      std::fclose (fp);
      std::remove (temp_path_cstr);
      return format_errno ("save failed: cannot write body");
    }
  if (std::fflush (fp) != 0)
    {
      std::fclose (fp);
      std::remove (temp_path_cstr);
      return format_errno ("save failed: cannot flush file");
    }
  if (std::fclose (fp) != 0)
    {
      std::remove (temp_path_cstr);
      return format_errno ("save failed: cannot close file");
    }

  std::remove (backup_path_cstr);

  bool moved = false;
  if (std::rename (path_cstr, backup_path_cstr) == 0)
    moved = true;
  else if (errno != ENOENT)
    {
      std::remove (temp_path_cstr);
      return format_errno ("save failed: cannot move old snapshot");
    }

  if (std::rename (temp_path_cstr, path_cstr) != 0)
    {
      if (moved)
	std::rename (backup_path_cstr, path_cstr);
      std::remove (temp_path_cstr);
      return format_errno ("save failed: cannot replace snapshot");
    }

  if (moved)
    std::remove (backup_path_cstr);

  return {};
}

result_type
load_file (std::string path, std::string &out)
{
  auto *fp = std::fopen (path.c_str (), "rb");
  if (fp == nullptr)
    return format_errno ("load failed: cannot open file");

  std::array<char, 4096> buffer;
  while (true)
    {
      auto n = std::fread (buffer.data (), 1, buffer.size (), fp);
      if (n != 0)
	out.append (buffer.data (), n);

      if (n == buffer.size ())
	continue;

      if (std::ferror (fp) != 0)
	{
	  std::fclose (fp);
	  return format_errno ("load failed: cannot read file");
	}
      break;
    }

  if (std::fclose (fp) != 0)
    return format_errno ("load failed: cannot close file");

  return {};
}

result_type
parse_value (std::int64_t type, resp::data &input, data &out)
{
  switch (type)
    {
    case type_string:
      {
	auto p = input.get_if<resp::bulk_string> ();
	if (p == nullptr || !p->has_value ())
	  return "load failed: invalid string value";
	out = data{ string{ std::move (p->value ()) } };
	return {};
      }

    case type_integer:
      {
	auto p = input.get_if<resp::integer> ();
	if (p == nullptr)
	  return "load failed: invalid integer value";
	out = data{ integer{ *p } };
	return {};
      }

    case type_list:
      {
	auto p = input.get_if<resp::array> ();
	if (p == nullptr || !p->has_value ())
	  return "load failed: invalid container value";
	list ls;
	auto &arr = p->value ();
	for (auto &item : arr)
	  {
	    auto p = item.get_if<resp::bulk_string> ();
	    if (p == nullptr || !p->has_value ())
	      return "load failed: invalid list element";
	    ls->push_back (std::move (p->value ()));
	  }
	out = data{ std::move (ls) };
	return {};
      }

    case type_set:
      {
	auto p = input.get_if<resp::array> ();
	if (p == nullptr || !p->has_value ())
	  return "load failed: invalid container value";
	set st;
	auto &arr = p->value ();
	for (auto &i : arr)
	  {
	    auto p = i.get_if<resp::bulk_string> ();
	    if (p == nullptr || !p->has_value ())
	      return "load failed: invalid set element";
	    st->insert (std::move (p->value ()));
	  }
	out = data{ std::move (st) };
	return {};
      }

    case type_hash:
      {
	auto p = input.get_if<resp::array> ();
	if (p == nullptr || !p->has_value ())
	  return "load failed: invalid container value";
	hashtable hs;
	auto &arr = p->value ();
	if (arr.size () % 2 != 0)
	  return "load failed: invalid hash length";
	for (std::size_t i = 0; i < arr.size (); i += 2)
	  {
	    auto pk = arr[i].get_if<resp::bulk_string> ();
	    auto pv = arr[i + 1].get_if<resp::bulk_string> ();
	    if (pk == nullptr || pv == nullptr || !pk->has_value ()
		|| !pv->has_value ())
	      return "load failed: invalid hash entry";
	    hs->insert_or_assign (std::move (pk->value ()),
				  std::move (pv->value ()));
	  }
	out = data{ std::move (hs) };
	return {};
      }

    default:
      return "load failed: unknown value type";
    }
}

result_type
parse_entry (resp::data &in, std::int64_t now, snapshot::entry &out,
	     bool &dropped)
{
  dropped = false;

  auto p = in.get_if<resp::array> ();
  if (p == nullptr || !p->has_value ())
    return "load failed: invalid snapshot entry";
  auto &arr = p->value ();
  if (arr.size () != 5)
    return "load failed: malformed snapshot entry";

  auto pk = arr[0].get_if<resp::bulk_string> ();
  if (pk == nullptr || !pk->has_value ())
    return "load failed: invalid snapshot key";
  std::string key{ std::move (pk->value ()) };

  auto pt = arr[1].get_if<resp::integer> ();
  if (pt == nullptr)
    return "load failed: invalid type tag";
  std::int64_t type = *pt;

  data value;
  auto value_res = parse_value (type, arr[2], value);
  if (!value_res.has_value ())
    return value_res;

  auto ph = arr[3].get_if<resp::integer> ();
  if (ph == nullptr)
    return "load failed: invalid expiration flag";
  std::int64_t has_expire = *ph;
  if (has_expire != 0 && has_expire != 1)
    return "load failed: invalid expiration flag";

  auto pe = arr[4].get_if<resp::integer> ();
  if (pe == nullptr)
    return "load failed: invalid expiration timestamp";
  std::int64_t expire_at_ms = *pe;

  optional<time_point> expire_at;
  if (has_expire == 0)
    {
      if (expire_at_ms != 0)
	return "load failed: malformed expiration fields";
    }
  else
    {
      if (expire_at_ms <= now)
	{
	  dropped = true;
	  return {};
	}
      expire_at = time_point{ milliseconds{ expire_at_ms } };
    }

  out = snapshot::entry{ std::move (key), std::move (value), expire_at };
  return {};
}

result_type
parse_body (string_view body, snapshot &out)
{
  resp::parser parser{ {} };
  parser.append_chunk (body);
  parser.parse ();

  if (parser.has_protocol_error ())
    {
      std::string s;
      parser.take_protocol_error (s);
      if (s.empty ())
	return "load failed: invalid RESP payload";
      return "load failed: " + s;
    }
  if (parser.available_data () != 1)
    return "load failed: invalid snapshot payload";

  auto root = parser.pop ();
  auto p = root.get_if<resp::array> ();
  if (p == nullptr || !p->has_value ())
    return "load failed: snapshot root is not an array";
  auto &arr = p->value ();

  auto now
      = duration_cast<milliseconds> (clock_type::now ().time_since_epoch ())
	    .count ();
  out.entries.clear ();
  out.entries.reserve (arr.size ());
  for (auto &i : arr)
    {
      bool dropped = false;
      snapshot::entry entry;
      auto entry_res = parse_entry (i, now, entry, dropped);
      if (!entry_res.has_value ())
	return entry_res;
      if (!dropped)
	out.entries.push_back (std::move (entry));
    }

  return {};
}

} // namespace

result<void, std::string>
save_to (string_view path, snapshot snap)
{
  std::string body;
  append_array_header (body, snap.entries.size ());
  for (const auto &entry : snap.entries)
    append_entry (body, entry);

  return save_file (path.to_string (), body);
}

result<void, std::string>
load_from (string_view path, snapshot &out)
{
  out.entries.clear ();

  std::string raw;
  auto res = load_file (path.to_string (), raw);
  if (!res.has_value ())
    return res;

  if (raw.size () < 5)
    return "load failed: file is too short";

  if (std::memcmp (raw.data (), format_magic, sizeof (format_magic)) != 0)
    return "load failed: bad format header";

  if (raw[4] != format_version)
    return "load failed: unsupported format version";

  return parse_body (string_view{ raw.data () + 5, raw.size () - 5 }, out);
}

} // namespace db
} // namespace mini_redis
