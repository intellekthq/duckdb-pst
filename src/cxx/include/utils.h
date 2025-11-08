#pragma once
#include <string>
#include <locale>
#include <codecvt>
#include <jank/runtime/obj/opaque_box.hpp>
#include <jank/c_api.h>

namespace duckpst::wstring {
    inline std::wstring from_utf8(const char* s) {
        std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> conv;
        return conv.from_bytes(s);
    }

    inline std::wstring from_utf8(const std::string& s) {
        return from_utf8(s.c_str());
    }

    inline std::string to_utf8(const std::wstring& ws) {
        std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> conv;
        return conv.to_bytes(ws);
    }
}

namespace duckpst::jank {
    using namespace ::jank::runtime;

    inline jtl::immutable_string get_box_type_str(object_ref o) {
        auto box = try_object<obj::opaque_box>(o);
        return box->canonical_type;
    }

    inline ::jank::u8 get_box_type(object_ref o) {
        return (::jank::u8) o->type;
    }
}
