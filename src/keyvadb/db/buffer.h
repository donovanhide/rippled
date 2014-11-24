#pragma once

#include <boost/optional.hpp>
#include <boost/optional/optional_io.hpp>
#include <boost/algorithm/hex.hpp>
#include <boost/bimap.hpp>
#include <boost/bimap/set_of.hpp>
#include <boost/bimap/multiset_of.hpp>
#include <boost/bimap/support/lambda.hpp>
#include <cassert>
#include <limits>
#include <string>
#include <map>
#include <ostream>
#include <mutex>
#include <algorithm>
#include "db/key.h"
#include "db/error.h"

namespace keyvadb
{
// A threadsafe container for storing keys and values for the period before they
// are committed to disk.
template <std::uint32_t BITS>
class Buffer
{
   public:
    enum class ValueState : std::uint8_t
    {
        Unprocessed,
        Evicted,
        NeedsCommitting,
        Committed,
    };

    struct Value
    {
        enum
        {
            Bytes = BITS / 8
        };

        std::uint64_t offset;
        std::uint32_t length;
        std::string value;
        ValueState status;

        bool ReadyForWriting() const
        {
            return status == ValueState::NeedsCommitting;
        }

        friend bool operator<(Value const &lhs, Value const &rhs)
        {
            if (lhs.status == rhs.status && lhs.offset == rhs.offset)
                return lhs.value < rhs.value;
            if (lhs.status == rhs.status)
                return lhs.offset < rhs.offset;
            return lhs.status < rhs.status;
        }
    };

   private:
    using util = detail::KeyUtil<BITS>;
    using key_type = typename util::key_type;
    using value_type = boost::optional<std::string>;
    using map_type = boost::bimap<boost::bimaps::set_of<key_type>,
                                  boost::bimaps::multiset_of<Value>>;
    using left_value_type = typename map_type::left_value_type;
    using candidate_type = std::set<KeyValue<BITS>>;

    static const std::string emptyBufferValue;
    static const std::map<ValueState, std::string> valueStates;
    static const std::uint32_t maxValueLength;

    map_type buf_;
    mutable std::mutex mtx_;

   public:
    value_type Get(std::string const &key) const
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto v = buf_.left.find(util::FromBytes(key));
        if (v != buf_.left.end() && v->second.status != ValueState::Evicted)
            // An Evicted key won't have an associated value
            return v->second.value;
        return boost::none;
    }

    std::size_t Add(std::string const &key, std::string const &value)
    {
        auto k = util::FromBytes(key);
        std::lock_guard<std::mutex> lock(mtx_);

        // Don't overwrite an existing key that might not be Unprocessed
        if (buf_.left.find(k) == buf_.left.end())
        {
            assert(value.length() <= maxValueLength);
            std::uint32_t length =
                value.size() + sizeof(std::uint32_t) + (BITS / 8);
            auto v = left_value_type(
                k, Value{0, length, value, ValueState::Unprocessed});
            buf_.left.insert(v);
        }
        return buf_.size();
    }

    std::size_t AddEvictee(key_type const &key, std::uint64_t const offset,
                           std::uint32_t const length)
    {
        std::lock_guard<std::mutex> lock(mtx_);
        assert(buf_.left.find(key) == buf_.left.end());
        auto v = left_value_type(
            key, Value{offset, length, emptyBufferValue, ValueState::Evicted});
        buf_.left.insert(v);
        return buf_.size();
    }

    void RemoveDuplicate(key_type const &key)
    {
        std::lock_guard<std::mutex> lock(mtx_);
        buf_.left.erase(key);
    }

    void SetOffset(key_type const &key, std::uint64_t const offset)
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = buf_.left.find(key);
        assert(it != buf_.left.end());
        auto v = Value{offset, it->second.length, it->second.value,
                       ValueState::NeedsCommitting};
        if (!buf_.left.modify_data(it, boost::bimaps::_data = v))
            throw std::runtime_error("Bad SetOffset");
    }

    bool Write(std::size_t const batchSize, std::vector<std::uint8_t> &wb)
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = first(ValueState::NeedsCommitting);
        if (it == buf_.right.end() ||
            it->first.status != ValueState::NeedsCommitting)
            return false;
        std::size_t pos = 0;
        // Add at least one key and value to the buffer.
        do
        {
            wb.resize(pos + it->first.length);
            std::memcpy(&wb[pos], &it->first.length, sizeof(it->first.length));
            pos += sizeof(it->first.length);
            auto b = util::ToBytes(it->second);
            std::memcpy(&wb[pos], b.data(), b.size());
            pos += b.size();
            std::memcpy(&wb[pos], it->first.value.data(),
                        it->first.value.size());
            pos += it->first.value.size();
            auto v = Value{it->first.offset, it->first.length, it->first.value,
                           ValueState::Committed};
            if (!buf_.right.modify_key(it, boost::bimaps::_key = v))
                throw std::runtime_error("Bad Buffer Write");
            it = first(ValueState::NeedsCommitting);
        } while (it != buf_.right.end() &&
                 it->first.status == ValueState::NeedsCommitting &&
                 pos + it->first.length <= batchSize);
        return true;
    }

    void Purge()
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto check = first(ValueState::NeedsCommitting);
        if (check != buf_.right.end() &&
            check->first.status == ValueState::NeedsCommitting)
            throw std::runtime_error("Bad Buffer Purge");
        buf_.right.erase(first(ValueState::Evicted), buf_.right.end());
    }

    void GetCandidates(key_type const &firstKey, key_type const &lastKey,
                       candidate_type &candidates, candidate_type &evictions)
    {
        std::lock_guard<std::mutex> lock(mtx_);
        std::for_each(lower(firstKey), upper(lastKey),
                      [&candidates, &evictions](left_value_type const &kv)
                      {

            if (kv.second.status == ValueState::Unprocessed)
                candidates.emplace(KeyValue<BITS>{kv.first, kv.second.offset,
                                                  kv.second.length});
            else if (kv.second.status == ValueState::Evicted)
                evictions.emplace(KeyValue<BITS>{kv.first, kv.second.offset,
                                                 kv.second.length});
        });
    }

    // Returns true if there are values greater than first and less than
    // last
    bool ContainsRange(key_type const &first, key_type const &last) const
    {
        assert(first <= last);
        std::lock_guard<std::mutex> lock(mtx_);
        return std::any_of(lower(first), upper(last),
                           [](left_value_type const &kv)
                           {
            return kv.second.status == ValueState::Unprocessed ||
                   kv.second.status == ValueState::Evicted;
        });
    }

    void Clear()
    {
        std::lock_guard<std::mutex> lock(mtx_);
        buf_.clear();
    }

    std::size_t Size() const
    {
        std::lock_guard<std::mutex> lock(mtx_);
        return buf_.size();
    }

    std::size_t ReadyForCommitting() const
    {
        std::lock_guard<std::mutex> lock(mtx_);
        return std::distance(first(ValueState::NeedsCommitting),
                             first(ValueState::Committed));
    }

    friend std::ostream &operator<<(std::ostream &stream, const Buffer &buffer)
    {
        stream << "Buffer" << std::endl;
        std::lock_guard<std::mutex> lock(buffer.mtx_);
        for (auto const &v : buffer.buf_.right)
            stream << util::ToHex(v.second) << ":" << v.first.offset << ":"
                   << v.first.length << ":" << valueStates.at(v.first.status)
                   << ":" << v.first.Size() << std::endl;
        stream << "--------" << std::endl;
        return stream;
    }

   private:
    typename map_type::left_const_iterator lower(key_type const &first) const
    {
        return buf_.left.upper_bound(first);
    }

    typename map_type::left_const_iterator upper(key_type const &last) const
    {
        return buf_.left.lower_bound(last);
    }

    typename map_type::right_const_iterator first(ValueState state) const
    {
        return buf_.right.lower_bound(Value{0, 0, emptyBufferValue, state});
    }

    typename map_type::right_iterator first(ValueState state)
    {
        return buf_.right.lower_bound(Value{0, 0, emptyBufferValue, state});
    }
};

template <std::uint32_t BITS>
const std::string Buffer<BITS>::emptyBufferValue;

template <std::uint32_t BITS>
const std::uint32_t Buffer<BITS>::maxValueLength =
    std::numeric_limits<std::uint32_t>::max() - sizeof(std::uint32_t) -
    (BITS / 8);

template <std::uint32_t BITS>
const std::map<typename Buffer<BITS>::ValueState, std::string>
    Buffer<BITS>::valueStates{
        {Buffer<BITS>::ValueState::Unprocessed, "Unprocessed"},
        {Buffer<BITS>::ValueState::Evicted, "Evicted"},
        {Buffer<BITS>::ValueState::NeedsCommitting, "NeedsCommitting"},
        {Buffer<BITS>::ValueState::Committed, "Committed"},
    };

}  // namespace keyvadb
