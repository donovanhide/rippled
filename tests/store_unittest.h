#include <string>
#include <tuple>
#include "tests/common.h"
#include "db/store.h"

using namespace keyvadb;

TYPED_TEST(StoreTest, SetAndGetKeys)
{
    auto first = this->MakeKey(0);
    auto last = this->FromHex('F');
    auto root = this->keys_->New(0, first, last);
    ASSERT_EQ(0UL, root->Id());
    ASSERT_EQ(first, root->First());
    ASSERT_EQ(last, root->Last());
    root->AddSyntheticKeyValues();
    ASSERT_TRUE(root->IsSane());
    std::error_condition err;
    auto node = this->EmptyNode();
    std::tie(node, err) = this->keys_->Get(root->Id());
    ASSERT_EQ(db_error::key_not_found, err.value());
    ASSERT_EQ(nullptr, node);
    ASSERT_FALSE(this->keys_->Set(root));
    std::tie(node, err) = this->keys_->Get(root->Id());
    ASSERT_FALSE(err);
    ASSERT_EQ(root->Last(), node->Last());
    ASSERT_TRUE(node->IsSane());
}

// TYPED_TEST(StoreTest, SetAndGetValues)
// {
//     auto key1 = this->FromHex('1');
//     auto key2 = this->FromHex('2');
//     std::string value1("First Value");
//     std::string value2("Second Value");
//     auto v1 = this->GetValue(0, value1);
//     auto v2 = this->GetValue(v1.length, value2);
//     ASSERT_FALSE(this->values_->Set(key1, v1));
//     ASSERT_FALSE(this->values_->Set(key2, v2));
//     std::string got1, got2;
//     ASSERT_FALSE(this->values_->Get(v1.offset, v1.length, &got1));
//     ASSERT_FALSE(this->values_->Get(v2.offset, v2.length, &got2));
//     ASSERT_EQ(value1, got1);
//     ASSERT_EQ(value2, got2);
// }

TYPED_TEST(StoreTest, Cache)
{
    this->cache_.SetMaxSize(2);
    auto first = this->MakeKey(0);
    auto last = this->FromHex('F');
    auto key1 = this->FromHex('1');
    auto key2 = this->FromHex('2');
    auto key4 = this->FromHex('4');
    auto key5 = this->FromHex('5');
    auto root = this->keys_->New(0, first, last);
    auto firstChild = this->keys_->New(1, key1, key5);
    auto secondChild = this->keys_->New(2, key2, key4);
    // The key 0000... can never be found in the cache
    ASSERT_FALSE(this->cache_.Get(first));
    this->cache_.Add(root);
    ASSERT_FALSE(this->cache_.Get(first));
    // The key 0000...0001 is the first key that can possibly be found
    ASSERT_EQ(this->cache_.Get(first + 1), root);
    ASSERT_EQ(this->cache_.GetById(root->Id()), root);
    this->cache_.Add(firstChild);
    ASSERT_EQ(this->cache_.Get(key1 + 1), firstChild);
    ASSERT_EQ(this->cache_.GetById(firstChild->Id()), firstChild);
    this->cache_.Add(secondChild);
    ASSERT_EQ(this->cache_.Get(key2 + 1), secondChild);
    ASSERT_EQ(this->cache_.GetById(secondChild->Id()), secondChild);
    // root now evicted
    ASSERT_FALSE(this->cache_.Get(first + 1));
    ASSERT_FALSE(this->cache_.GetById(0));

    this->cache_.Add(firstChild);
    this->cache_.Add(firstChild);
    // std::cout << this->cache_;
}
