#include "tests/common.h"
#include "db/tree.h"

using namespace keyvadb;

TYPED_TEST(StoreTest, TreeOperations)
{
    auto tree = this->GetTree();
    ASSERT_FALSE(tree->Init(false));
    // Check root has been created
    this->checkTree(tree);
    ASSERT_NE(0UL, this->keys_->Size());
    // Insert some random values
    // twice with same seed to insert duplicates
    const std::size_t n = 20;
    const std::size_t rounds = 4;
    for (std::size_t i = 0; i < 2; i++)
    {
        for (std::size_t j = 0; j < rounds; j++)
        {
            // Use j as seed
            auto input = this->RandomKeyValues(n, j);
            for (auto const& kv : input) this->buffer_.Add(kv.first, kv.second);
            // std::cout << "Add" << this->buffer_;
            ASSERT_EQ(n, this->buffer_.Size());
            auto journal = this->GetJournal();
            ASSERT_FALSE(journal->Process(*tree));
            // std::cout << "Process" << this->buffer_;
            this->checkTree(tree);
            // std::cout << this->buffer_;
            ASSERT_FALSE(journal->Commit(*tree, 5));
            // std::cout << "Commit" << this->buffer_;
            this->checkTree(tree);
            this->CheckRandomKeyValues(tree, n, j);
            // std::cout << *tree;
        }
    }
    // std::cout << this->cache_;
}
