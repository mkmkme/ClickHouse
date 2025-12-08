#pragma once

#include "config.h"

#if USE_SSL

#include <Common/Exception.h>

#include <openssl/crypto.h>
#include <openssl/evp.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int OPENSSL_ERROR;
}

namespace OpenSSLOptimized
{

class EVPContextPool
{
public:
    struct ContextWrapper
    {
        EVP_CIPHER_CTX * ctx = nullptr;

        ContextWrapper()
        {
            ctx = EVP_CIPHER_CTX_new();
            if (!ctx)
                throw Exception(ErrorCodes::OPENSSL_ERROR, "Failed to create EVP_CIPHER_CTX");
        }

        ~ContextWrapper()
        {
            if (ctx)
                EVP_CIPHER_CTX_free(ctx);
        }

        ContextWrapper(const ContextWrapper &) = delete;
        ContextWrapper & operator=(const ContextWrapper &) = delete;

        void reset() const
        {
            EVP_CIPHER_CTX_reset(ctx);
        }
    };

    static ContextWrapper & getContext()
    {
        thread_local ContextWrapper context;
        return context;
    }
};

class CipherCache
{
    struct CacheEntry
    {
        EVP_CIPHER * cipher = nullptr;

        ~CacheEntry()
        {
            if (cipher)
                EVP_CIPHER_free(cipher);
        }
    };

    // std::unordered_map<std::string, CacheEntry> cache;
    std::unordered_map<std::string, std::unique_ptr<CacheEntry>> cache;
    std::mutex mutex;

public:

    const EVP_CIPHER * getCipher(const std::string & cipher_name)
    {
        // TODO: original code uses two critical sections
        std::lock_guard<std::mutex> lock(mutex);
        auto it = cache.find(cipher_name);
        if (it != cache.end())
            return it->second->cipher;

        EVP_CIPHER * cipher = EVP_CIPHER_fetch(nullptr, cipher_name.c_str(), nullptr);
        if (!cipher)
            return nullptr;

        // cache.emplace(std::piecewise_construct, std::forward_as_tuple(cipher_name), std::forward_as_tuple(cipher));
        cache.emplace(cipher_name, std::make_unique<CacheEntry>(cipher));

        return cipher;
    }

    static CipherCache & instance()
    {
        static CipherCache instance;
        return instance;
    }
};

inline const EVP_CIPHER * getCipherByNameFast(std::string_view cipher_name)
{
    return CipherCache::instance().getCipher(std::string(cipher_name));
}

}

}

#endif
