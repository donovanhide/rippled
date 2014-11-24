#pragma once

#include <algorithm>
#include <string>
#include <unordered_map>
#include <utility>
#include "db/key.h"
#include "db/node.h"
#include "db/env.h"
#include "db/encoding.h"
#include "db/error.h"

namespace keyvadb
{
template <std::uint32_t BITS>
class ValueStore
{
    using util = detail::KeyUtil<BITS>;
    using key_type = typename util::key_type;
    using file_type = std::unique_ptr<RandomAccessFile>;
    using key_value_func =
        std::function<void(std::string const&, std::string const&)>;

   private:
    enum
    {
        Bytes = BITS / 8
    };
    file_type file_;
    std::atomic_uint_fast64_t size_;
    static const std::size_t value_offset;

   public:
    explicit ValueStore(file_type& file) : file_(std::move(file)) {}
    ValueStore(const ValueStore&) = delete;
    ValueStore& operator=(const ValueStore&) = delete;

    std::error_condition Open()
    {
        if (auto err = file_->OpenAppend())
            return err;
        return file_->Size(size_);
    }
    std::error_condition Clear()
    {
        size_ = 0;
        return file_->Truncate();
    }
    std::error_condition Close() { return file_->Close(); }
    std::error_condition Get(std::uint64_t const offset,
                             std::uint32_t const length,
                             std::string* value) const
    {
        value->resize(length - value_offset);
        if (value->size() == 0)
            throw std::runtime_error("zero length read");
        std::size_t bytesRead;
        std::error_condition err;
        std::tie(bytesRead, err) = file_->ReadAt(offset + value_offset, *value);
        if (err)
            return err;
        if (bytesRead < value->length())
            return make_error_condition(db_error::short_read);
        return std::error_condition();
    }

    std::error_condition Append(std::vector<std::uint8_t> const& buf)
    {
        std::size_t bytesWritten;
        std::error_condition err;
        std::tie(bytesWritten, err) = file_->Write(buf);
        if (err)
            return err;
        if (bytesWritten != buf.size())
            return make_error_condition(db_error::short_write);
        size_ += bytesWritten;
        return std::error_condition();
    }

    std::error_condition Each(key_value_func f) const
    {
        std::string str(1024 * 64, '\0');
        std::uint64_t filePosition = 0;
        std::string key, value;
        do
        {
            std::error_condition err;
            std::size_t bytesRead;
            std::tie(bytesRead, err) = file_->ReadAt(filePosition, str);
            if (err)
                return err;
            std::uint32_t length = 0;
            for (std::size_t pos = 0; pos < bytesRead;)
            {
                string_read<std::uint32_t>(str, pos, length);
                // tail case
                if (pos + length > bytesRead)
                {
                    break;
                }
                pos += sizeof(length);
                key.assign(str, pos, Bytes);
                pos += Bytes;
                auto valueLength = length - value_offset;
                value.assign(str, pos, valueLength);
                pos += valueLength;
                f(key, value);
                filePosition += length;
            }
        } while (filePosition < size_);
        return std::error_condition();
    }

    std::uint64_t Size() const { return size_; }
};
template <std::uint32_t BITS>
const std::size_t ValueStore<BITS>::value_offset = util::MaxSize() +
                                                   sizeof(std::uint32_t);

template <std::uint32_t BITS>
class KeyStore
{
    using util = detail::KeyUtil<BITS>;
    using key_type = typename util::key_type;
    using node_type = Node<BITS>;
    using node_ptr = std::shared_ptr<node_type>;
    using node_result = std::pair<node_ptr, std::error_condition>;
    using file_type = std::unique_ptr<RandomAccessFile>;

   private:
    const std::uint32_t block_size_;
    const std::uint32_t degree_;
    file_type file_;
    std::atomic_uint_fast64_t size_;

   public:
    KeyStore(std::uint32_t const block_size, file_type& file)
        : block_size_(block_size),
          degree_(node_type::CalculateDegree(block_size)),
          file_(std::move(file))
    {
    }
    KeyStore(const KeyStore&) = delete;
    KeyStore& operator=(const KeyStore&) = delete;

    std::error_condition Open()
    {
        if (auto err = file_->Open())
            return err;
        return file_->Size(size_);
    }

    std::error_condition Clear()
    {
        size_ = 0;
        return file_->Truncate();
    }

    std::error_condition Close() { return file_->Close(); }

    node_ptr New(std::uint32_t const level, key_type const& first,
                 key_type const& last)
    {
        size_ += block_size_;
        return std::make_shared<node_type>(size_ - block_size_, level, degree_,
                                           first, last);
    }

    node_result Get(std::uint64_t const id) const
    {
        std::string str;
        str.resize(block_size_);
        auto node = std::make_shared<node_type>(id, 0, degree_, 0, 1);
        std::size_t bytesRead;
        std::error_condition err;
        std::tie(bytesRead, err) = file_->ReadAt(id, str);
        if (err)
            return std::make_pair(node_ptr(), err);
        if (bytesRead == 0)
        {
            return std::make_pair(
                node_ptr(), make_error_condition(db_error::key_not_found));
        }
        if (bytesRead != block_size_)
            return std::make_pair(node_ptr(),
                                  make_error_condition(db_error::short_read));
        node->Read(str);
        return std::make_pair(node, std::error_condition());
    }

    std::error_condition Set(node_ptr const& node)
    {
        std::string str;
        str.resize(block_size_);
        node->Write(str);
        std::size_t bytesWritten;
        std::error_condition err;
        std::tie(bytesWritten, err) = file_->WriteAt(str, node->Id());
        if (err)
            return err;
        if (bytesWritten != block_size_)
            return make_error_condition(db_error::short_write);
        return std::error_condition();
    }

    std::uint64_t Size() const { return size_; }
};

template <std::uint32_t BITS>
static std::unique_ptr<KeyStore<BITS>> CreateKeyStore(
    std::string const& filename, std::uint32_t const blockSize)
{
    // Put ifdef here!
    auto file = std::unique_ptr<RandomAccessFile>(
        std::make_unique<PosixRandomAccessFile>(filename));
    // endif
    return std::make_unique<KeyStore<BITS>>(blockSize, file);
}

template <std::uint32_t BITS>
static std::unique_ptr<ValueStore<BITS>> CreateValueStore(
    std::string const& filename)
{
    // Put ifdef here!
    auto file = std::unique_ptr<RandomAccessFile>(
        std::make_unique<PosixRandomAccessFile>(filename));
    // endif
    return std::make_unique<ValueStore<BITS>>(file);
}

}  // namespace keyvadb
