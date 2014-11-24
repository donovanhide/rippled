#pragma once

#include <cstdint>
#include <cstddef>
#include <set>
#include <algorithm>
#include "db/buffer.h"

namespace keyvadb
{
template <std::uint32_t BITS>
class Delta
{
    using util = detail::KeyUtil<BITS>;
    using key_type = typename util::key_type;
    using node_type = Node<BITS>;
    using node_ptr = std::shared_ptr<node_type>;
    using buffer_type = Buffer<BITS>;

   private:
    std::uint64_t existing_;
    std::uint64_t insertions_;
    std::uint64_t evictions_;
    std::uint64_t synthetics_;
    std::uint64_t children_;
    node_ptr current_;
    node_ptr previous_;

   public:
    explicit Delta(node_ptr const& node)
        : existing_(0),
          insertions_(0),
          evictions_(0),
          synthetics_(0),
          children_(0),
          current_(node)
    {
    }

    void Flip()
    {
        // Copy on write
        if (!Dirty())
        {
            previous_ = current_;
            current_ = std::make_shared<node_type>(*previous_);
        }
    }

    constexpr bool Dirty() const { return previous_ ? true : false; }
    constexpr node_ptr Current() const { return current_; }
    constexpr std::uint64_t Insertions() const
    {
        return insertions_ - evictions_;
    }

    bool CheckSanity() { return current_->IsSane(); }

    void SetChild(std::size_t const i, std::uint64_t const cid)
    {
        Flip();
        children_++;
        current_->SetChild(i, cid);
    }

    std::uint64_t AddKeys(buffer_type& buffer, std::uint64_t offset)
    {
        auto N = current_->MaxKeys();
        std::set<KeyValue<BITS>> candidates;
        std::set<KeyValue<BITS>> evictions;
        buffer.GetCandidates(current_->First(), current_->Last(), candidates,
                             evictions);
        if (candidates.size() + evictions.size() == 0)
        {
            // Nothing to do, this is the root node being checked for work
            return offset;
        }
        std::set<KeyValue<BITS>> existing(current_->NonZeroBegin(),
                                          current_->keys.cend());

        existing_ = existing.size();

        std::vector<KeyValue<BITS>> dupes;
        std::set_intersection(candidates.cbegin(), candidates.cend(),
                              existing.cbegin(), existing.cend(),
                              std::back_inserter(dupes));
        // Remove dupes
        for (auto const& kv : dupes)
        {
            buffer.RemoveDuplicate(kv.key);
            candidates.erase(kv);
        }
        if ((candidates.size() == 0 && evictions.size() == 0) ||
            current_->EmptyKeyCount() == 0)
            // Nothing to do
            return offset;

        Flip();
        if (existing.size() + candidates.size() + evictions.size() <= N)
        {
            // Won't overflow copy and sort
            insertions_ = candidates.size();
            auto lastCandidate = std::copy(
                candidates.cbegin(), candidates.cend(), current_->keys.begin());
            std::copy(evictions.cbegin(), evictions.cend(), lastCandidate);
            for (auto it = current_->keys.begin(); it != lastCandidate; ++it)
            {
                insertions_++;
                buffer.SetOffset(it->key, offset);
                it->offset = offset;
                offset += it->length;
            }
            std::sort(current_->keys.begin(), current_->keys.end());
            return offset;
        }

        // Handle overflowing node
        std::set<KeyValue<BITS>> combined(candidates);
        std::copy(evictions.cbegin(), evictions.cend(),
                  std::inserter(combined, combined.end()));
        std::copy(existing.cbegin(), existing.cend(),
                  std::inserter(combined, combined.end()));
        current_->Clear();
        auto stride = current_->Stride();
        std::size_t index = 0;
        auto best = util::Max();
        for (auto const& kv : combined)
        {
            std::uint32_t nearest;
            key_type distance;
            util::NearestStride(current_->First(), stride, kv.key, distance,
                                nearest);
            if ((nearest == index && distance < best) || (nearest != index))
            {
                current_->SetKeyValue(nearest, kv);
                best = distance;
            }
            index = nearest;
        }
        synthetics_ = current_->AddSyntheticKeyValues();
        for (auto& kv : current_->keys)
        {
            if (kv.IsSynthetic())
                continue;
            if (candidates.count(kv) > 0)
            {
                insertions_++;
                buffer.SetOffset(kv.key, offset);
                kv.offset = offset;
                offset += kv.length;
            }
            existing.erase(kv);
        }
        for (auto const& kv : existing)
        {
            if (kv.IsSynthetic())
                continue;
            evictions_++;
            buffer.AddEvictee(kv.key, kv.offset, kv.length);
        }
        return offset;
    }

    friend std::ostream& operator<<(std::ostream& stream, const Delta& delta)
    {
        stream << "Id: " << std::setw(12) << delta.current_->Id()
               << " Existing: " << std::setw(3) << delta.existing_
               << " Insertions: " << std::setw(3) << delta.insertions_
               << " Evictions: " << std::setw(3) << delta.evictions_
               << " Synthetics: " << std::setw(3) << delta.synthetics_
               << " Children: " << std::setw(3) << delta.children_;
        return stream;
    }
};
}  // namespace keyvadb
