#pragma once

#include "pstsdk/ltp/propbag.h"
#include "pstsdk/ltp/table.h"
#include "pstsdk/util/primitives.h"
#include "pstsdk/util/util.h"
#include <codecvt>
#include <locale>
#include <string>

namespace intellekt::duckpst::utils {
inline std::wstring to_wstring(std::string s) {
	// deprecated, replacement TBD
	std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
	return converter.from_bytes({s.begin(), s.end()});
}

inline std::string to_utf8(std::wstring s) {
	// deprecated, replacement TBD
	std::wstring_convert<std::codecvt_utf8<wchar_t>> converter;
	return converter.to_bytes({s.begin(), s.end()});
}

// TODO: Tries to read a string out of a pbag with fallbacks. Apparently many PST
// writers are careless with prop_type.
inline std::string read_prop_utf8(pstsdk::property_bag &bag, pstsdk::prop_id id) {
	if (!bag.prop_exists(id))
		return nullptr;

	auto buf = bag.read_prop<std::vector<pstsdk::byte>>(id);
	if (bag.get_prop_type(id) == pstsdk::prop_type_wstring) {
		return pstsdk::bytes_to_string(buf);
	} else {
		return std::string(buf.begin(), buf.end());
	}
}

inline std::string read_prop_utf8(pstsdk::const_table_row &bag, pstsdk::prop_id id) {
	if (!bag.prop_exists(id))
		return nullptr;

	auto buf = bag.read_prop<std::vector<pstsdk::byte>>(id);
	if (bag.get_prop_type(id) == pstsdk::prop_type_wstring) {
		return pstsdk::bytes_to_string(buf);
	} else {
		return std::string(buf.begin(), buf.end());
	}
}

} // namespace intellekt::duckpst::utils
