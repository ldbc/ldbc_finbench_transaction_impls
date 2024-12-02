//
// Created by yxy on 4/3/23.
//

#ifndef GPSTORE_UTIL_FOR_CACHE_H
#define GPSTORE_UTIL_FOR_CACHE_H
#include <cstdlib>
#include <memory>
#include <utility>
#define CACHE_LINE_SIZE 64

void* cacheline_aligned_alloc(unsigned size);

enum CacheMetadataChargePolicy {
        // Only the `charge` of each entry inserted into a Cache counts against
        // the `capacity`
        kDontChargeCacheMetadata,
        // In addition to the `charge`, the approximate space overheads in the
        // Cache (in bytes) also count against `capacity`. These space overheads
        // are for supporting fast Lookup and managing the lifetime of entries.
        kFullChargeCacheMetadata
    };
const CacheMetadataChargePolicy kDefaultCacheMetadataChargePolicy =
            kFullChargeCacheMetadata;

struct CompactStr
{
    std::shared_ptr<char[]> value = nullptr;
    unsigned len = 0;
    CompactStr(std::shared_ptr<char[]> v, unsigned l):value(std::move(v)), len(l){}
    CompactStr(char* v, unsigned l):len(l)
    {
        value.reset(v);
    }
    CompactStr()=default;
};

#endif //GPSTORE_UTIL_FOR_CACHE_H
