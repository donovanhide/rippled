#pragma once

#include <string>
#include <boost/algorithm/hex.hpp>
#include "gtest/gtest.h"

#include "db/key.h"
#include "db/node.h"
#include "db/tree.h"
#include "db/store.h"
#include "db/buffer.h"
#include "db/journal.h"
#include "db/db.h"

using namespace keyvadb;

::testing::AssertionResult NoError(std::error_condition err)
{
    if (err)
        return ::testing::AssertionFailure() << err.message();
    return ::testing::AssertionSuccess();
}

// Simple wrapper around BITS
template <std::uint32_t BITS>
struct TestPolicy
{
    enum
    {
        Bits = BITS
    };
};

template <typename TestPolicy>
class StoreTest : public ::testing::Test,
                  public detail::KeyUtil<TestPolicy::Bits>
{
   protected:
    using key_store_ptr = std::unique_ptr<KeyStore<TestPolicy::Bits>>;
    using value_store_ptr = std::unique_ptr<ValueStore<TestPolicy::Bits>>;
    using node_ptr = std::shared_ptr<Node<TestPolicy::Bits>>;
    using value_type = typename Buffer<TestPolicy::Bits>::Value;
    using status_type = typename Buffer<TestPolicy::Bits>::ValueState;
    using key_value_type = KeyValue<TestPolicy::Bits>;
    using tree_type = Tree<TestPolicy::Bits>;
    using tree_ptr = std::unique_ptr<tree_type>;
    using cache_type = NodeCache<TestPolicy::Bits>;
    using journal_type = Journal<TestPolicy::Bits>;
    using journal_ptr = std::unique_ptr<journal_type>;
    using buffer_type = Buffer<TestPolicy::Bits>;
    using random_type = std::vector<std::pair<std::string, std::string>>;

    key_store_ptr keys_;
    value_store_ptr values_;
    cache_type cache_;
    buffer_type buffer_;

    StoreTest()
        : keys_(CreateKeyStore<TestPolicy::Bits>("test.keys", 4096)),
          values_(CreateValueStore<TestPolicy::Bits>("test.values"))
    {
    }

    virtual void SetUp()
    {
        ASSERT_FALSE(keys_->Open());
        ASSERT_FALSE(keys_->Clear());
        ASSERT_FALSE(values_->Open());
        ASSERT_FALSE(values_->Clear());
        buffer_.Clear();
        cache_.Reset();
    }

    virtual void TearDown()
    {
        ASSERT_FALSE(keys_->Close());
        ASSERT_FALSE(values_->Close());
    }

    node_ptr EmptyNode() { return nullptr; }

    value_type GetValue(std::uint64_t const offset, std::string const& value)
    {
        return value_type{offset,
                          std::uint32_t(value.size() + sizeof(std::uint32_t) +
                                        TestPolicy::Bits / 8),
                          value, status_type::NeedsCommitting};
    }

    key_value_type EmptyKeyValue() { return key_value_type(); }

    // Fills a binary key with garbage hex
    std::string HexString(char const c)
    {
        return std::string(TestPolicy::Bits / 8, c);
    }

    tree_ptr GetTree() { return std::make_unique<tree_type>(*keys_, cache_); }

    journal_ptr GetJournal()
    {
        return std::make_unique<journal_type>(buffer_, *values_);
    }

    random_type RandomKeyValues(std::size_t const n, std::uint32_t const seed)
    {
        random_type pairs;
        for (auto const& key : this->RandomKeys(n, seed))
        {
            auto keyBytes = this->ToBytes(key);
            pairs.emplace_back(keyBytes, keyBytes);
        }
        return pairs;
    }

    void CheckRandomKeyValues(tree_ptr const& tree, std::size_t n,
                              std::uint32_t seed)
    {
        for (auto const& key : this->RandomKeys(n, seed))
        {
            key_value_type got;
            std::error_condition err;
            std::tie(got, err) = tree->Get(key);
            ASSERT_FALSE(err);
            ASSERT_EQ(key, got.key);
        }
    }

    void checkTree(tree_ptr const& tree)
    {
        bool sane;
        std::error_condition err;
        std::tie(sane, err) = tree->IsSane();
        ASSERT_FALSE(err);
        ASSERT_TRUE(sane);
    }

    void checkCount(tree_ptr const& tree, std::size_t const expected)
    {
        std::size_t count;
        std::error_condition err;
        std::tie(count, err) = tree->NonSyntheticKeyCount();
        ASSERT_FALSE(err);
        ASSERT_EQ(expected, count);
    }

    void checkValue(tree_ptr const& tree, KeyValue<256> const kv)
    {
        key_value_type got;
        std::error_condition err;
        std::tie(got, err) = tree->Get(kv.key);
        ASSERT_FALSE(err);
        ASSERT_EQ(kv, got);
    }
};

template <typename TestPolicy>
class DBTest : public ::testing::Test
{
   public:
    using util = detail::KeyUtil<TestPolicy::Bits>;

    std::unique_ptr<DB<TestPolicy::Bits>> GetDB()
    {
        Options options;
        options.keyFileName = "db.test.keys";
        options.valueFileName = "db.test.values";
        return std::make_unique<DB<TestPolicy::Bits>>(options);
    }

    auto RandomKeys(std::size_t n, std::uint32_t seed)
    {
        std::vector<std::string> keys;
        for (auto const& key : util::RandomKeys(n, seed))
            keys.emplace_back(util::ToBytes(key));
        return keys;
    }

    void CompareKeys(std::string const& a, std::string const& b)
    {
        ASSERT_EQ(a, b) << boost::algorithm::hex(a)
                        << " != " << boost::algorithm::hex(b);
    }
};

typedef ::testing::Types<TestPolicy<256>> DBTypes;
TYPED_TEST_CASE(DBTest, DBTypes);

typedef ::testing::Types<TestPolicy<256>> StoreTypes;
TYPED_TEST_CASE(StoreTest, StoreTypes);