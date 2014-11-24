#pragma once

#include <cstdint>
#include <cstddef>
#include <unordered_map>
#include <system_error>
#include <utility>
#include "db/key.h"
#include "db/node.h"
#include "db/store.h"
#include "db/buffer.h"
#include "db/cache.h"
#include "db/delta.h"
#include "db/error.h"

namespace keyvadb
{
template <uint32_t BITS>
class Tree
{
   public:
    using util = detail::KeyUtil<BITS>;
    using key_type = typename util::key_type;
    using key_value_type = KeyValue<BITS>;
    using key_store_type = KeyStore<BITS>;
    using node_ptr = std::shared_ptr<Node<BITS>>;
    using node_func =
        std::function<std::error_condition(node_ptr, std::uint32_t)>;
    using cache_type = NodeCache<BITS>;

   private:
    static const uint64_t rootId = 0;
    key_store_type& store_;
    cache_type& cache_;

   public:
    Tree(key_store_type& store, cache_type& cache)
        : store_(store), cache_(cache)
    {
    }

    // Build root node if not already present
    std::error_condition Init(bool const addSynthetics)
    {
        node_ptr root;
        std::error_condition err;
        std::tie(root, err) = store_.Get(rootId);
        if (!err)
            return err;
        root = store_.New(0, firstRootKey(), lastRootKey());
        if (addSynthetics)
            root->AddSyntheticKeyValues();
        cache_.Reset();
        cache_.Add(root);
        return store_.Set(root);
    }

    std::error_condition Walk(node_func f) const { return walk(rootId, 0, f); }

    std::pair<node_ptr, std::error_condition> Root() const
    {
        return GetNode(rootId);
    }

    std::pair<node_ptr, std::error_condition> GetNode(std::uint64_t id) const
    {
        auto node = cache_.GetById(id);
        if (node)
            return std::make_pair(node, std::error_condition());
        return store_.Get(id);
    }

    node_ptr CreateNode(std::uint32_t const level, key_type const& first,
                        key_type const& last)
    {
        return store_.New(level, first, last);
    }

    std::pair<key_value_type, std::error_condition> Get(
        key_type const& key) const
    {
        auto node = cache_.Get(key);
        if (!node)
        {
            std::error_condition err;
            std::tie(node, err) = store_.Get(rootId);
            if (err)
                throw std::runtime_error("no root!");
        }
        return get(node, key);
    }

    std::error_condition Update(const node_ptr& node)
    {
        if (auto err = store_.Set(node))
            return err;
        cache_.Add(node);
        return std::error_condition();
    }

    std::pair<bool, std::error_condition> IsSane() const
    {
        bool sane = true;
        auto err = Walk([&sane](node_ptr n, std::uint32_t)
                        {
                            sane &= n->IsSane();
                            return std::error_condition();
                        });
        return std::make_pair(sane, err);
    }

    std::pair<std::size_t, std::error_condition> NonSyntheticKeyCount() const
    {
        std::size_t count = 0;
        auto err = Walk([&count](node_ptr n, std::uint32_t)
                        {
                            count += n->NonSyntheticKeyCount();
                            return std::error_condition();
                        });
        return std::make_pair(count, err);
    }

    friend std::ostream& operator<<(std::ostream& stream, const Tree& tree)
    {
        auto err = tree.Walk([&stream](node_ptr n, std::uint32_t level)
                             {
                                 stream << "Level:\t\t" << level << std::endl
                                        << *n;
                                 return std::error_condition();
                             });
        if (err)
            stream << err.message() << std::endl;
        return stream;
    }

   private:
    static constexpr key_type firstRootKey() { return util::Min() + 1; }
    static constexpr key_type lastRootKey() { return util::Max(); }

    std::pair<key_value_type, std::error_condition> get(
        node_ptr const& node, key_type const& key) const
    {
        key_value_type kv;
        if (node->Find(key, &kv))
            return std::make_pair(kv, std::error_condition());
        // TODO(DH) This needs early breaking to be efficient
        bool found = false;
        auto err = node->EachChild(
            [&](const std::size_t, const key_type& first, const key_type& last,
                const std::uint64_t cid)
            {
                if (key > first && key < last)
                {
                    if (cid == EmptyChild)
                    {
                        return make_error_condition(db_error::key_not_found);
                    }
                    found = true;
                    node_ptr node;
                    std::error_condition err;
                    std::tie(node, err) = store_.Get(cid);
                    if (err)
                    {
                        return err;
                    }
                    cache_.Add(node);
                    std::tie(kv, err) = get(node, key);
                    return err;
                }
                return std::error_condition();
            });
        if (!found)
            err = db_error::key_not_found;
        return std::make_pair(kv, err);
    }

    std::error_condition walk(std::uint64_t const id, std::uint32_t const level,
                              node_func f) const
    {
        node_ptr node;
        std::error_condition err;
        std::tie(node, err) = store_.Get(id);
        if (err)
            return err;
        if (auto err = f(node, level))
            return err;
        return node->EachChild([&](const std::size_t, const key_type&,
                                   const key_type&, const std::uint64_t cid)
                               {
                                   if (cid != EmptyChild)
                                       if (auto err = walk(cid, level + 1, f))
                                           return err;
                                   return std::error_condition();
                               });
    }
};

}  // namespace keyvadb
