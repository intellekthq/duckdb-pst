#pragma once

#include "pstsdk/ltp/propbag.h"
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

// Tries to read a string out of a pbag with fallbacks. Apparently many PST
// writers are careless with prop_type.
inline std::string read_prop_utf8(pstsdk::property_bag &bag, pstsdk::prop_id id) {
	try {
		auto wide = bag.read_prop<std::wstring>(id);
		return to_utf8(wide);
	} catch (...) {
		// TODO: surface some level of validation errors to the user with
		//       each row, in an optional column
	}

	// Read bytes and shove them into a regular string as a last resort
	auto buf = bag.read_prop<std::vector<pstsdk::byte>>(id);
	std::wstring_convert<std::codecvt_utf8<wchar_t>> converter;
	return converter.to_bytes({buf.begin(), buf.end()});
}

} // namespace intellekt::duckpst::utils
