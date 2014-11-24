#pragma once

#include <boost/optional.hpp>
#include <boost/algorithm/hex.hpp>
#include <string>
#include <map>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <system_error>
#include "db/store.h"
#include "db/buffer.h"
#include "db/tree.h"
#include "db/journal.h"
#include "db/log.h"

namespace keyvadb
{
struct Options
{
    // Size of a node on disk, which determines the degree of the node.
    std::uint32_t blockSize = 4096;

    // Number of nodes to cache in memory.
    // Default is 1GB of memory for default blockSize.
    std::uint64_t cacheSize = 1024 * 1024 * 1024 / 4096;

    // Approximate maximum size of each write in the flush process.
    std::uint64_t writeBufferSize = 1024 * 1024;

    // Time between each flush to disk in milliseconds
    std::uint32_t flushInterval = 1000;

    // Path and name of the file to store the key index.
    std::string keyFileName = "db.keys";

    // Path and name of the file to store the keys and values.
    std::string valueFileName = "db.values";
};

template <std::uint32_t BITS, class Log = NullLog>
class DB
{
    using util = detail::KeyUtil<BITS>;
    using key_store_ptr = std::unique_ptr<KeyStore<BITS>>;
    using value_store_ptr = std::unique_ptr<ValueStore<BITS>>;
    using key_value_type = KeyValue<BITS>;
    using buffer_type = Buffer<BITS>;
    using journal_type = Journal<BITS>;
    using tree_type = Tree<BITS>;
    using cache_type = NodeCache<BITS>;
    using key_value_func =
        std::function<void(std::string const &, std::string const &)>;

    enum
    {
        key_length = BITS / 8
    };

    const Options options_;
    Log log_;
    key_store_ptr keys_;
    value_store_ptr values_;
    cache_type cache_;
    tree_type tree_;
    buffer_type buffer_;
    std::atomic_uint_fast64_t buffer_hits_;
    std::atomic_uint_fast64_t key_misses_;
    std::atomic_uint_fast64_t value_hits_;
    std::atomic_uint_fast64_t value_misses_;
    std::atomic<bool> close_;
    std::thread thread_;

   public:
    DB(Options const &options)
        : options_(options),
          log_(Log{}),
          keys_(CreateKeyStore<BITS>(options.keyFileName, options.blockSize)),
          values_(CreateValueStore<BITS>(options.valueFileName)),
          cache_(),
          tree_(*keys_, cache_),
          buffer_hits_(0),
          key_misses_(0),
          value_hits_(0),
          value_misses_(0),
          close_(false),
          thread_(&DB::flushThread, this)
    {
        cache_.SetMaxSize(options.cacheSize);
    }
    DB(DB const &) = delete;
    DB &operator=(DB const &) = delete;

    ~DB()
    {
        close_ = true;
        thread_.join();
        if (auto err = values_->Close())
            if (log_.error)
                log_.error << "Closing values: " << err.message();
        if (auto err = keys_->Close())
            if (log_.error)
                log_.error << "Closing keys: " << err.message();
    }

    // Not threadsafe
    std::error_condition Open()
    {
        if (auto err = keys_->Open())
            return err;
        if (auto err = tree_.Init(true))
            return err;
        return values_->Open();
    }

    // Not threadsafe
    std::error_condition Clear()
    {
        buffer_.Clear();
        if (auto err = keys_->Clear())
            return err;
        if (auto err = tree_.Init(true))
            return err;
        return values_->Clear();
    }

    std::error_condition Get(std::string const &key, std::string *value)
    {
        if (key.length() != key_length)
            return db_error::key_wrong_length;
        if (auto v = buffer_.Get(key))
        {
            if (v->length() == 0)
                throw std::runtime_error("Bad Get");
            value->assign(*v);
            buffer_hits_++;
            return std::error_condition();
        }
        // Value must be on disk
        key_value_type kv;
        std::error_condition err;
        std::tie(kv, err) = tree_.Get(util::FromBytes(key));
        if (err)
        {
            key_misses_++;
            return err;
        }
        if (kv.length == 0)
            throw std::runtime_error("Bad length for: " +
                                     boost::algorithm::hex(key));
        err = values_->Get(kv.offset, kv.length, value);
        if (err)
            value_misses_++;
        else
            value_hits_++;
        return err;
    }

    std::error_condition Put(std::string const &key, std::string const &value)
    {
        if (key.length() != key_length)
            return db_error::key_wrong_length;
        if (value.size() > std::numeric_limits<std::uint32_t>::max())
            return db_error::value_too_long;
        if (value.size() == 0)
            return db_error::zero_length_value;
        buffer_.Add(key, value);
        // if ( buffer_.Add(key, value) >10000)
        // naive rate limiter to stop the buffer growing too fast
        // Consider: http://en.wikipedia.org/wiki/Token_bucket
        // std::this_thread::sleep_for(std::chrono::microseconds(10));
        return std::error_condition();
    }

    // Returns keys and values in insertion order
    std::error_condition Each(key_value_func f) { return values_->Each(f); }

   private:
    std::error_condition flush()
    {
        journal_type journal(buffer_, *values_);
        if (auto err = journal.Process(tree_))
            return err;
        if (log_.info)
            log_.info << "Flushing: " << buffer_.ReadyForCommitting() << "/"
                      << buffer_.Size() << " keys into " << journal.Size()
                      << " nodes Buffer hits: " << buffer_hits_
                      << " Key misses: " << key_misses_
                      << " Value Hits: " << value_hits_
                      << " Value Misses: " << value_misses_ << " Cache "
                      << cache_.ToString();
        return journal.Commit(tree_, options_.writeBufferSize);
    }

    void flushThread()
    {
        for (;;)
        {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(options_.flushInterval));
            bool stop = close_;
            if (auto err = flush())
                if (log_.error)
                    log_.error << "Flushing Error: " << err.message() << ":"
                               << err.category().name();
            if (stop)
                break;
        }
        // thread exits
    }
};
}  // namespace keyvadb
