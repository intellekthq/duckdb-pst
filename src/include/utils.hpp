#pragma once

#include <string>
#include <vector>
#include <pstsdk/util.h>

namespace intellekt::duckpst::utils {
    inline std::wstring to_wstring(std::string s) {
        std::vector<pstsdk::byte> bs(s.begin(), s.end());
        bs.push_back('\0');
        return pstsdk::bytes_to_wstring(bs);
    }
}