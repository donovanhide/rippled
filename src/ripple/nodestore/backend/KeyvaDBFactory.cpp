//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2012, 2013 Ripple Labs Inc.

    Permission to use, copy, modify, and/or distribute this software for any
    purpose  with  or without fee is hereby granted, provided that the above
    copyright notice and this permission notice appear in all copies.

    THE  SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
    WITH  REGARD  TO  THIS  SOFTWARE  INCLUDING  ALL  IMPLIED  WARRANTIES  OF
    MERCHANTABILITY  AND  FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
    ANY  SPECIAL ,  DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
    WHATSOEVER  RESULTING  FROM  LOSS  OF USE, DATA OR PROFITS, WHETHER IN AN
    ACTION  OF  CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
    OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/
//==============================================================================

#if RIPPLE_KEYVADB_AVAILABLE

#include <ripple/core/Config.h>

namespace ripple
{
namespace NodeStore
{
class KeyvaDBBackend : public Backend,
                       public BatchWriter::Callback,
                       public beast::LeakChecked<LevelDBBackend>
{
   public:
    beast::Journal m_journal;
    size_t const m_keyBytes;
    Scheduler& m_scheduler;
    BatchWriter m_batch;
    std::string m_name;
    // std::unique_ptr<
    //     keyvadb::DB<keyvadb::FileStoragePolicy<256>, keyvadb::StandardLog>>
    //     m_db;
    std::unique_ptr<keyvadb::DB<keyvadb::FileStoragePolicy<256>>> m_db;

    KeyvaDBBackend(int keyBytes, Parameters const& keyValues,
                   Scheduler& scheduler, beast::Journal journal)
        : m_journal(journal),
          m_keyBytes(keyBytes),
          m_scheduler(scheduler),
          m_batch(*this, scheduler),
          m_name(keyValues["path"].toStdString())
    {
        if (m_name.empty())
            throw std::runtime_error("Missing path in KeyvaDBFactory backend");

        std::uint32_t block_size = 4096;
        if (!keyValues["block_size"].isEmpty())
        {
            block_size = keyValues["block_size"].getIntValue();
        }
        std::uint64_t cache_size = (1024 * 1024 * 1024) / block_size;  // 1 GB
        if (!keyValues["cache_size"].isEmpty())
        {
            cache_size = keyValues["cache_size"].getIntValue();
        }
        // m_db = std::make_unique<
        //     keyvadb::DB<keyvadb::FileStoragePolicy<256>,
        //     keyvadb::StandardLog>>(
        //     m_name + "db.keys", m_name + "db.values", block_size,
        //     cache_size);
        m_db = std::make_unique<keyvadb::DB<keyvadb::FileStoragePolicy<256>>>(
            m_name + "db.keys", m_name + "db.values", block_size, cache_size);
        if (auto err = m_db->Open())
            throw std::runtime_error(
                std::string("Unable to open/create keyvadb: ") + err.message());
    }

    std::string getName() { return m_name; }

    //--------------------------------------------------------------------------

    Status fetch(void const* key, NodeObject::Ptr* pObject)
    {
        pObject->reset();
        std::string value;
        std::string k(std::string(static_cast<char const*>(key), m_keyBytes));
        auto err = m_db->Get(k, &value);
        if (err == keyvadb::db_error::key_not_found ||
            err == keyvadb::db_error::value_not_found)
            return Status(notFound);
        if (err)
            return Status(unknown);
        DecodedBlob decoded(key, value.data(), value.size());
        if (!decoded.wasOk())
            return Status(dataCorrupt);
        *pObject = decoded.createObject();
        return Status(ok);
    }

    void store(NodeObject::ref object)
    {
        EncodedBlob encoded;
        encoded.prepare(object);
        std::string key(reinterpret_cast<char const*>(encoded.getKey()),
                        m_keyBytes);
        std::string value(reinterpret_cast<char const*>(encoded.getData()),
                          encoded.getSize());
        if (auto err = m_db->Put(key, value))
            throw std::runtime_error("storeBatch failed: " + err.message());
    }

    void storeBatch(Batch const& batch)
    {
        for (auto const& e : batch) store(e);
    }

    void for_each(std::function<void(NodeObject::Ptr)> f)
    {
        m_db->Each([&](std::string key, std::string value)
                   {
                       DecodedBlob decoded(key.data(), value.data(),
                                           value.size());
                       if (decoded.wasOk())
                       {
                           f(decoded.createObject());
                       }
                       else
                       {
                           // Uh oh, corrupted data!
                           if (m_journal.fatal)
                               m_journal.fatal << "Corrupt NodeObject #"
                                               << uint256(key.data());
                       }
                   });
    }

    int getWriteLoad() { return m_batch.getWriteLoad(); }

    //--------------------------------------------------------------------------

    void writeBatch(Batch const& batch) { storeBatch(batch); }
};

//------------------------------------------------------------------------------

class KeyvaDBFactory : public Factory
{
   public:
    class BackendImp;

    KeyvaDBFactory() {}

    ~KeyvaDBFactory() {}

    std::string getName() const { return "KeyvaDB"; }

    std::unique_ptr<Backend> createInstance(size_t keyBytes,
                                            Parameters const& keyValues,
                                            Scheduler& scheduler,
                                            beast::Journal journal)
    {
        return std::make_unique<KeyvaDBBackend>(keyBytes, keyValues, scheduler,
                                                journal);
    }
};

//------------------------------------------------------------------------------

std::unique_ptr<Factory> make_KeyvaDBFactory()
{
    return std::make_unique<KeyvaDBFactory>();
}
}
}

#endif