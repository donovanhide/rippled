#include "tests/common.h"
#include "db/buffer.h"

using namespace keyvadb;

TEST(BufferTest, General)
{
    using util = detail::KeyUtil<256>;
    Buffer<256> buffer;
    // Add some known keys
    auto first = util::MakeKey(1);
    auto last = util::FromHex('F');
    auto ones = util::FromHex('1');
    auto threes = util::FromHex('3');
    buffer.Add(util::ToBytes(ones), "ones");
    buffer.Add(util::ToBytes(ones + 1), "ones plus one");
    buffer.Add(util::ToBytes(ones - 1), "ones minus one");
    buffer.Add(util::ToBytes(ones + 2), "ones plus two");
    buffer.Add(util::ToBytes(ones - 2), "ones minus two");
    buffer.Add(util::ToBytes(threes), "threes");

    // ContainsRange
    ASSERT_TRUE(buffer.ContainsRange(first, last));
    ASSERT_FALSE(buffer.ContainsRange(first, first));
    ASSERT_FALSE(buffer.ContainsRange(last, last));
    ASSERT_TRUE(buffer.ContainsRange(ones, threes));
    ASSERT_FALSE(buffer.ContainsRange(ones, ones + 1));
    ASSERT_FALSE(buffer.ContainsRange(ones - 1, ones));
    ASSERT_TRUE(buffer.ContainsRange(ones, ones + 2));
    ASSERT_TRUE(buffer.ContainsRange(ones - 2, ones));
    // std::cout << buffer;
}

TEST(BufferTest, Purge)
{
    using util = detail::KeyUtil<256>;
    Buffer<256> buffer;
    ASSERT_EQ(0UL, buffer.Size());
    auto keys = util::RandomKeys(10, 0);
    // Add some keys with every other value having an offset
    std::uint64_t offset = 0;
    for (auto const& key : keys)
    {
        buffer.Add(util::ToBytes(key), util::ToBytes(key));
        if (offset % 2 == 0)
            buffer.SetOffset(key, offset);
        offset++;
    }
    ASSERT_EQ(keys.size(), buffer.Size());
    // std::cout << buffer;
    // buffer.Purge();
    // std::cout << buffer;
}
