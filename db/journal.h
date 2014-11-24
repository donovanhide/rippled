#pragma once

#include <cstdint>
#include <cstddef>
#include <map>
#include <cassert>
#include <system_error>
#include "db/key.h"
#include "db/node.h"
#include "db/tree.h"
#include "db/buffer.h"
#include "db/store.h"
#include "db/delta.h"

namespace keyvadb
{
// Journal is the where all changes to the keys and values occur
// and the rollback file is created.
template <std::uint32_t BITS>
class Journal
{
    using util = detail::KeyUtil<BITS>;
    using key_type = typename util::key_type;
    using value_store_type = ValueStore<BITS>;
    using key_value_type = KeyValue<BITS>;
    using delta_type = Delta<BITS>;
    using node_ptr = std::shared_ptr<Node<BITS>>;
    using tree_type = Tree<BITS>;
    using buffer_type = Buffer<BITS>;

   private:
    buffer_type& buffer_;
    value_store_type& values_;
    std::multimap<std::uint32_t, delta_type> deltas_;
    std::uint64_t offset_;

   public:
    Journal(buffer_type& buffer, value_store_type& values)
        : buffer_(buffer), values_(values)
    {
    }

    std::error_condition Process(tree_type& tree)
    {
        offset_ = values_.Size();
        std::error_condition err;
        node_ptr root;
        std::tie(root, err) = tree.Root();
        if (err)
            return err;
        return process(tree, root);
    }

    std::error_condition Commit(tree_type& tree, std::size_t const batchSize)
    {
        // This is where the rollback file creation should go!
        // -->

        // This should build an iovec and use writev instead
        std::vector<std::uint8_t> writeBuffer;
        writeBuffer.reserve(batchSize);
        while (buffer_.Write(batchSize, writeBuffer))
        {
            if (auto err = values_.Append(writeBuffer))
                return err;
        }
        // write deepest nodes first so that no parent can refer
        // to a non-existent child
        for (auto it = deltas_.crbegin(), end = deltas_.crend(); it != end;
             ++it)
            if (auto err = tree.Update(it->second.Current()))
                return err;
        buffer_.Purge();
        deltas_.clear();
        return std::error_condition();
    }

    constexpr std::size_t Size() const { return deltas_.size(); }

    std::uint64_t TotalInsertions() const
    {
        std::uint64_t total = 0;
        for (auto const& kv : deltas_) total += kv.second.Insertions();
        return total;
    }

    friend std::ostream& operator<<(std::ostream& stream,
                                    const Journal& journal)
    {
        for (auto const& kv : journal.deltas_)
        {
            std::cout << "Level: " << std::setw(3) << kv.first << " "
                      << kv.second << std::endl;
        }

        return stream;
    }

   private:
    std::error_condition process(tree_type& tree, node_ptr const& node)
    {
        delta_type delta(node);
        offset_ = delta.AddKeys(buffer_, offset_);
        assert(delta.CheckSanity());
        if (delta.Current()->EmptyKeyCount() == 0)
        {
            auto err = delta.Current()->EachChild(
                [&](const std::size_t i, const key_type& first,
                    const key_type& last, const std::uint64_t cid)
                {
                    if (!buffer_.ContainsRange(first, last))
                        return std::error_condition();
                    if (cid == EmptyChild)
                    {
                        auto child =
                            tree.CreateNode(node->Level() + 1, first, last);
                        delta.SetChild(i, child->Id());
                        return process(tree, child);
                    }
                    else
                    {
                        node_ptr child;
                        std::error_condition err;
                        std::tie(child, err) = tree.GetNode(cid);
                        if (err)
                            return err;
                        return process(tree, child);
                    }
                });
            if (err)
                return err;
        }
        assert(delta.CheckSanity());
        if (delta.Dirty())
            deltas_.emplace(node->Level(), delta);
        return std::error_condition();
    }
};
}  // namespace keyvadb
