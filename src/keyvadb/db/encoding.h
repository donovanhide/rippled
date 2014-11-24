#pragma once
#include <string>

template <class T>
std::size_t string_replace(T const& t, std::size_t const pos, std::string& s)
{
    std::memcpy(&s[pos], &t, sizeof(T));
    return sizeof(T);
}

template <class T>
std::size_t string_read(std::string const& s, std::size_t const pos, T& t)
{
    std::memcpy(&t, &s[pos], sizeof(T));
    return sizeof(T);
}
