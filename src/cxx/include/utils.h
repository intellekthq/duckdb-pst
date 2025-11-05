#pragma once
#include <string>
#include <locale>
#include <codecvt>

inline std::wstring utf8_to_wstring(const char* s) {
    std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> conv;
    return conv.from_bytes(s);
}

inline std::wstring utf8_to_wstring(const std::string& s) {
    return utf8_to_wstring(s.c_str());
}

inline std::string wstring_to_utf8(const std::wstring& ws) {
    std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> conv;
    return conv.to_bytes(ws);
}
