#pragma once

#include <boost/multiprecision/cpp_int.hpp>
#include <boost/multiprecision/random.hpp>
#include <boost/math/tools/precision.hpp>
#include <cstdint>
#include <cstddef>
#include <vector>
#include <string>
#include <limits>

namespace keyvadb
{
static const std::uint64_t EmptyKey = 0;

static const std::uint64_t SyntheticValue =
    std::numeric_limits<std::uint64_t>::max();
static const std::uint64_t EmptyValue = 0;

namespace detail
{
template <std::uint32_t BITS>
struct KeyUtil
{
    using key_type =
        boost::multiprecision::number<boost::multiprecision::cpp_int_backend<
            BITS, BITS, boost::multiprecision::unsigned_magnitude,
            boost::multiprecision::checked, void>>;
    using seed_type = boost::random::mt19937;
    using gen_type =
        boost::random::independent_bits_engine<seed_type, BITS, key_type>;

    enum
    {
        Bits = BITS,
        HexChars = BITS / 4,
        Bytes = BITS / 8
    };

    static key_type MakeKey(const std::uint64_t num) { return key_type(num); }

    static key_type FromHex(char const c)
    {
        return FromHex(std::string(HexChars, c));
    }

    static key_type FromHex(std::size_t const count, char const c)
    {
        return FromHex(std::string(count, c));
    }

    static key_type FromHex(std::string const& s) { return key_type("0x" + s); }

    static std::string ToHex(key_type const& key)
    {
        std::stringstream ss;
        ss << std::setw(BITS / 4) << std::setfill('0') << std::setbase(16)
           << key;
        return ss.str();
    }

    static std::string ToBytes(key_type const& key)
    {
        auto bytes = key.backend().limbs();
        auto length = key.backend().size() * sizeof(*key.backend().limbs());
        auto str = std::string(reinterpret_cast<const char*>(bytes), length);
        std::reverse(str.begin(), str.end());
        return str;
    }

    static key_type FromBytes(std::string const& str)
    {
        key_type key;
        auto length = str.size() / sizeof(*key.backend().limbs());
        key.backend().resize(length, length);
        auto bytes = key.backend().limbs();
        std::reverse_copy(str.cbegin(), str.cend(),
                          reinterpret_cast<char*>(bytes));
        key.backend().normalize();
        return key;
    }

    static std::size_t WriteBytes(key_type const& key, const std::size_t pos,
                                  std::string& str)
    {
        static const auto maxLength = MaxSize();
        auto bytes = key.backend().limbs();
        auto limbLength = key.backend().size() * sizeof(*key.backend().limbs());
        // Copy limbs into rightmost position, probably not portable
        auto offset = maxLength - limbLength;
        std::memcpy(&str[pos], bytes, maxLength - offset);
        return maxLength;
    }

    static std::size_t ReadBytes(std::string const& str, const std::size_t pos,
                                 key_type& key)
    {
        static const auto maxLength = MaxSize();
        auto limbLength = maxLength / sizeof(*key.backend().limbs());
        // Make sure we have enough limbs for largest possible value
        key.backend().resize(limbLength, limbLength);
        auto bytes = key.backend().limbs();
        std::memcpy(bytes, &str[pos], maxLength);
        // TODO(DH) Find out why this is necessary
        key.backend().normalize();
        return maxLength;
    }

    static key_type Distance(key_type const& a, key_type const& b)
    {
        if (a > b)
            return a - b;
        return b - a;
    }

    static key_type Stride(key_type const& start, key_type const& end,
                           std::uint32_t const& n)
    {
        return (end - start) / n;
    }

    static void NearestStride(key_type const& start, key_type const& stride,
                              key_type const& value, key_type& distance,
                              std::uint32_t& nearest)
    {
        key_type index;
        divide_qr(value - start, stride, index, distance);
        nearest = static_cast<std::uint32_t>(index);
        // Round up first
        if (nearest == 0)
        {
            nearest++;
            distance = stride - distance;
        }
        nearest--;
    }

    static const key_type Max()
    {
        return boost::math::tools::max_value<key_type>();
    }

    static const key_type Min()
    {
        return boost::math::tools::min_value<key_type>();
    }

    static std::size_t MaxSize()
    {
        auto max = Max();
        return max.backend().size() * sizeof(*max.backend().limbs());
    }

    static std::vector<key_type> RandomKeys(std::size_t n, std::uint32_t seed)
    {
        seed_type base(seed);
        gen_type gen(base);
        std::vector<key_type> v;
        for (std::size_t i = 0; i < n; i++) v.emplace_back(gen());
        return v;
    }
};
}  // namespace detail

template <std::uint32_t BITS>
struct KeyValue
{
    using util = detail::KeyUtil<BITS>;
    using key_type = typename util::key_type;

    key_type key;          // Hash of actual value
    std::uint64_t offset;  // offset of actual value in values file
    std::uint32_t length;  // length of entry in values file

    constexpr bool IsZero() const { return key.is_zero(); }
    constexpr bool IsSynthetic() const { return offset == SyntheticValue; }
    constexpr bool operator<(KeyValue<BITS> const& rhs) const
    {
        return key < rhs.key;
    }
    constexpr bool operator==(KeyValue<BITS> const& rhs) const
    {
        return key == rhs.key;
    }
    constexpr bool operator!=(KeyValue<BITS> const& rhs) const
    {
        return key != rhs.key;
    }
    friend std::ostream& operator<<(std::ostream& stream,
                                    KeyValue<BITS> const& kv)
    {
        stream << "Key: " << util::ToHex(kv.key) << " Value: " << kv.offset;
        return stream;
    }
};
}  // namespace keyvadb
