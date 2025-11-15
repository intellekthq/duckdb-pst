#pragma once

#include <codecvt>
#include <locale>
#include <string>

namespace intellekt::duckpst::utils {
inline std::wstring to_wstring(std::string s) {
	// deprecated, replacement TBD
	std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
	return converter.from_bytes({s.begin(), s.end()});
}

inline std::string from_wstring(std::wstring s) {
	// deprecated, replacement TBD
	std::wstring_convert<std::codecvt_utf8<wchar_t>> converter;
	return converter.to_bytes({s.begin(), s.end()});
}
} // namespace intellekt::duckpst::utils
