#include <boost/algorithm/hex.hpp>
#include <string>
#include "tests/common.h"
#include "db/key.h"

using namespace keyvadb;
using boost::algorithm::unhex;

template <typename T>
class KeyTest : public ::testing::Test
{
   public:
    T policy_;
};

typedef ::testing::Types<detail::KeyUtil<1024>, detail::KeyUtil<256>,
                         detail::KeyUtil<32>, detail::KeyUtil<8>> KeyUtilTypes;
TYPED_TEST_CASE(KeyTest, KeyUtilTypes);

TYPED_TEST(KeyTest, General)
{
    auto zero = this->policy_.MakeKey(0);
    auto two = this->policy_.MakeKey(2);
    auto first = this->policy_.MakeKey(1);
    auto last = this->policy_.FromHex('F');
    auto ones = this->policy_.FromHex('1');
    auto twos = this->policy_.FromHex('2');
    auto threes = this->policy_.FromHex('3');
    // Min/Max
    ASSERT_EQ(zero, this->policy_.Min());
    ASSERT_EQ(last, this->policy_.Max());
    // Comparisons
    ASSERT_TRUE(zero.is_zero());
    ASSERT_TRUE(first < last);
    ASSERT_TRUE(last > first);
    ASSERT_TRUE(first != last);
    // Adddition
    ASSERT_EQ(threes, ones + twos);
    // Exceptions
    ASSERT_THROW(last + first, std::overflow_error);
    ASSERT_THROW(first - last, std::range_error);
    ASSERT_THROW(this->policy_.FromHex(this->policy_.HexChars + 2, 'F'),
                 std::overflow_error);
    // Distances
    ASSERT_EQ(ones, this->policy_.Distance(threes, twos));
    ASSERT_EQ(ones, this->policy_.Distance(twos, threes));
    // Strides
    auto stride = this->policy_.Stride(zero, last, 15);
    ASSERT_EQ(ones, stride);
    // Nearest
    uint32_t nearest;
    auto distance = this->policy_.MakeKey(0);
    this->policy_.NearestStride(zero, stride, ones, distance, nearest);
    ASSERT_EQ(zero, distance);
    ASSERT_EQ(0UL, nearest);
    this->policy_.NearestStride(zero, stride, twos, distance, nearest);
    ASSERT_EQ(zero, distance);
    ASSERT_EQ(1UL, nearest);
    this->policy_.NearestStride(zero, stride, two, distance, nearest);
    ASSERT_EQ(ones - two, distance);
    ASSERT_EQ(0UL, nearest);
    // From/To bytes
    auto f = this->policy_.ToBytes(first);
    auto f2 = this->policy_.FromBytes(f);
    ASSERT_EQ(first, f2);
    auto l = this->policy_.ToBytes(last);
    auto l2 = this->policy_.FromBytes(l);
    ASSERT_EQ(last, l2);
    // Read/Write string
    std::string s;
    s.resize(this->policy_.Bits / 8);
    this->policy_.WriteBytes(first, 0, s);
    auto readKey = this->policy_.MakeKey(0);
    this->policy_.ReadBytes(s, 0, readKey);
    ASSERT_EQ(first, readKey);
}

TEST(KeyTest, RoundTrip)
{
    using util = detail::KeyUtil<256>;
    using key_type = typename util::key_type;
    std::string in(
        "1E0DABB20AAAC3498DE92C73EA14E0FAB24BE2F53E503A0ACEB73AD54DB8DBF5");
    auto key = util::FromBytes(unhex(in));
    ASSERT_EQ(in, util::ToHex(key));
}
