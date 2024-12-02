//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// APIs for customizing read caches in RocksDB.

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>


#include "slice.h"




    class Logger;
    class SecondaryCacheResultHandle;
    class Statistics;

// A Cache maps keys to objects resident in memory, tracks reference counts
// on those key-object entries, and is able to remove unreferenced entries
// whenever it wants. All operations are fully thread safe except as noted.
// Inserted entries have a specified "charge" which is some quantity in
// unspecified units, typically bytes of memory used. A Cache will typically
// have a finite capacity in units of charge, and evict entries as needed
// to stay at or below that capacity.
//
// NOTE: This API is for expert use only and is intended more for customizing
// cache behavior than for calling into outside of RocksDB. It is subject to
// change as RocksDB evolves, especially the RocksDB block cache. Overriding
// CacheWrapper is the preferred way of customizing some operations on an
// existing implementation.
//
// INTERNAL: See typed_cache.h for convenient wrappers on top of this API.
// New virtual functions must also be added to CacheWrapper below.
    class Cache {
    public:  // types hidden from API client
        // Opaque handle to an entry stored in the cache.
        struct Handle {};

    public:  // types hidden from Cache implementation
        // Pointer to cached object of unspecified type. (This type alias is
        // provided for clarity, not really for type checking.)
        using ObjectPtr = uint8_t*;  /// yexinyi edit it.

        // Opaque object providing context (settings, etc.) to create objects
        // for primary cache from saved (serialized) secondary cache entries.
        struct CreateContext {};

    public:  // type defs
        // Depending on implementation, cache entries with higher priority levels
        // could be less likely to get evicted than entries with lower priority
        // levels. The "high" priority level applies to certain SST metablocks (e.g.
        // index and filter blocks) if the option
        // cache_index_and_filter_blocks_with_high_priority is set. The "low" priority
        // level is used for other kinds of SST blocks (most importantly, data
        // blocks), as well as the above metablocks in case
        // cache_index_and_filter_blocks_with_high_priority is
        // not set. The "bottom" priority level is for BlobDB's blob values.
        enum class Priority { HIGH, LOW, BOTTOM };


    public:  // ctor/dtor/create
//        Cache(std::shared_ptr<MemoryAllocator> allocator = nullptr)
//                : memory_allocator_(std::move(allocator)) {}
        Cache() = default;
        // No copying allowed
        Cache(const Cache&) = delete;
        Cache& operator=(const Cache&) = delete;

        // Destroys all remaining entries by calling the associated "deleter"
        virtual ~Cache() {}

        // Creates a new Cache based on the input value string and returns the result.
        // Currently, this method can be used to create LRUCaches only
        // @param config_options
        // @param value  The value might be:
        //   - an old-style cache ("1M") -- equivalent to NewLRUCache(1024*102(
        //   - Name-value option pairs -- "capacity=1M; num_shard_bits=4;
        //     For the LRUCache, the values are defined in LRUCacheOptions.
        // @param result The new Cache object
        // @return OK if the cache was successfully created
        // @return NotFound if an invalid name was specified in the value
        // @return InvalidArgument if either the options were not valid
//        static Status CreateFromString(const ConfigOptions& config_options,
//                                       const std::string& value,
//                                       std::shared_ptr<Cache>* result);

    public:  // functions
        // The type of the Cache
        virtual const char* Name() const = 0;

        // The Insert and Lookup APIs below are intended to allow cached objects
        // to be demoted/promoted between the primary block cache and a secondary
        // cache. The secondary cache could be a non-volatile cache, and will
        // likely store the object in a different representation. They rely on a
        // per object CacheItemHelper to do the conversions.
        // The secondary cache may persist across process and system restarts,
        // and may even be moved between hosts. Therefore, the cache key must
        // be repeatable across restarts/reboots, and globally unique if
        // multiple DBs share the same cache and the set of DBs can change
        // over time.

        // Insert a mapping from key->object into the cache and assign it
        // the specified charge against the total cache capacity. If
        // strict_capacity_limit is true and cache reaches its full capacity,
        // return Status::MemoryLimit. `obj` must be non-nullptr if compatible
        // with secondary cache (helper->size_cb != nullptr), because Value() ==
        // nullptr is reserved for indicating some secondary cache failure cases.
        // On success, returns OK and takes ownership of `obj`, eventually deleting
        // it with helper->del_cb. On non-OK return, the caller maintains ownership
        // of `obj` so will often need to delete it in such cases.
        //
        // The helper argument is saved by the cache and will be used when the
        // inserted object is evicted or considered for promotion to the secondary
        // cache. Promotion to secondary cache is only enabled if helper->size_cb
        // != nullptr. The helper must outlive the cache. Callers may use
        // &kNoopCacheItemHelper as a trivial helper (no deleter for the object,
        // no secondary cache). `helper` must not be nullptr (efficiency).
        //
        // If `handle` is not nullptr and return status is OK, `handle` is set
        // to a Handle* for the entry. The caller must call this->Release(handle)
        // when the returned entry is no longer needed. If `handle` is nullptr, it is
        // as if Release is called immediately after Insert.
        //
        // Regardless of whether the item was inserted into the cache,
        // it will attempt to insert it into the secondary cache if one is
        // configured, and the helper supports it.
        // The cache implementation must support a secondary cache, otherwise
        // the item is only inserted into the primary cache. It may
        // defer the insertion to the secondary cache as it sees fit.
        //
        // When the inserted entry is no longer needed, it will be destroyed using
        // helper->del_cb (if non-nullptr).
//        virtual void Insert(const Slice& key, ObjectPtr obj,
//                              /*const CacheItemHelper* helper,*/ size_t charge,
//                              Handle** handle = nullptr,
//                              Priority priority = Priority::LOW) = 0;


        // Lookup the key, returning nullptr if not found. If found, returns
        // a handle to the mapping that must eventually be passed to Release().
        //
        // If a non-nullptr helper argument is provided with a non-nullptr
        // create_cb, and a secondary cache is configured, then the secondary
        // cache is also queried if lookup in the primary cache fails. If found
        // in secondary cache, the provided create_db and create_context are
        // used to promote the entry to an object in the primary cache.
        // In that case, the helper may be saved and used later when the object
        // is evicted, so as usual, the pointed-to helper must outlive the cache.
//        virtual Handle* Lookup(const Slice& key,
//                               Priority priority = Priority::LOW,
//                               Statistics* stats = nullptr) = 0;


        // If the cache contains the entry for the key, erase it.  Note that the
        // underlying entry will be kept around until all existing handles
        // to it have been released.
//        virtual void Erase(const Slice& key) = 0;
        // Return a new numeric id.  May be used by multiple clients who are
        // sharding the same cache to partition the key space.  Typically the
        // client will allocate a new id at startup and prepend the id to
        // its cache keys.
        virtual uint64_t NewId() = 0;

        // sets the maximum configured capacity of the cache. When the new
        // capacity is less than the old capacity and the existing usage is
        // greater than new capacity, the implementation will do its best job to
        // purge the released entries from the cache in order to lower the usage
        virtual void SetCapacity(size_t capacity) = 0;

        // Set whether to return error on insertion when cache reaches its full
        // capacity.
        virtual void SetStrictCapacityLimit(bool strict_capacity_limit) = 0;

        // Get the flag whether to return error on insertion when cache reaches its
        // full capacity.
        virtual bool HasStrictCapacityLimit() const = 0;

        // Returns the maximum configured capacity of the cache
        virtual size_t GetCapacity() const = 0;


    };



// Useful for cache entries requiring no clean-up, such as for cache
// reservations
//    extern const Cache::CacheItemHelper kNoopCacheItemHelper;


