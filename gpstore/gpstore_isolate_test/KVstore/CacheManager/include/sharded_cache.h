//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <mutex>

#include "advanced_cache.h"
#include "hash.h"
#include "util_for_cache.h"
#include "../../DiskIV/DiskIV.h"


// Optional base class for classes implementing the CacheShard concept
    class CacheShardBase {
    public:
        explicit CacheShardBase(CacheMetadataChargePolicy metadata_charge_policy)
                : metadata_charge_policy_(metadata_charge_policy) {}

//        using DeleterFn = Cache::DeleterFn;

        // Expected by concept CacheShard (TODO with C++20 support)
        // Some Defaults
        std::string GetPrintableOptions() const { return ""; }
        using HashVal = uint64_t;
        using HashCref = uint64_t;
        static inline HashVal ComputeHash(const Slice& key) {
            return GetSliceNPHash64(key);
        }
        static inline uint32_t HashPieceForSharding(HashCref hash) {
            return Lower32of64(hash);
        }
        void AppendPrintableOptions(std::string& /*str*/) const {}


    protected:
        const CacheMetadataChargePolicy metadata_charge_policy_;
    };

// Portions of ShardedCache that do not depend on the template parameter
    class ShardedCacheBase : public Cache {
    public:
        ShardedCacheBase(size_t capacity, int num_shard_bits,
                         bool strict_capacity_limit);
        virtual ~ShardedCacheBase() = default;

        int GetNumShardBits() const;
        uint32_t GetNumShards() const;

        uint64_t NewId() override;

        bool HasStrictCapacityLimit() const override;
        size_t GetCapacity() const override;
//
//        using Cache::GetUsage;
//        size_t GetUsage(Handle* handle) const override;
//        std::string GetPrintableOptions() const override;

    protected:  // fns
        virtual void AppendPrintableOptions(std::string& str) const = 0;
        size_t GetPerShardCapacity() const;
        size_t ComputePerShardCapacity(size_t capacity) const;

    protected:                        // data
        std::atomic<uint64_t> last_id_;  // For NewId
        const uint32_t shard_mask_;

        // Dynamic configuration parameters, guarded by config_mutex_
        bool strict_capacity_limit_;
        size_t capacity_;
        mutable std::mutex config_mutex_;
    };

// Generic cache interface that shards cache by hash of keys. 2^num_shard_bits
// shards will be created, with capacity split evenly to each of the shards.
// Keys are typically sharded by the lowest num_shard_bits bits of hash value
// so that the upper bits of the hash value can keep a stable ordering of
// table entries even as the table grows (using more upper hash bits).
// See CacheShardBase above for what is expected of the CacheShard parameter.
    template <class CacheShard>
    class ShardedCache : public ShardedCacheBase {
    public:
        using HashVal = typename CacheShard::HashVal;
        using HashCref = typename CacheShard::HashCref;
        using HandleImpl = typename CacheShard::HandleImpl;

        ShardedCache(size_t capacity, int num_shard_bits, bool strict_capacity_limit)
                : ShardedCacheBase(capacity, num_shard_bits, strict_capacity_limit),
                  shards_(reinterpret_cast<CacheShard*>(cacheline_aligned_alloc(
                          sizeof(CacheShard) * GetNumShards()))),
                  destroy_shards_in_dtor_(false) {}

        virtual ~ShardedCache() {
            if (destroy_shards_in_dtor_) {
                ForEachShard([](CacheShard* cs) { cs->~CacheShard(); });
            }
            free(shards_);
        }

        CacheShard& GetShard(HashCref hash) {
            return shards_[CacheShard::HashPieceForSharding(hash) & shard_mask_];
        }

        const CacheShard& GetShard(HashCref hash) const {
            return shards_[CacheShard::HashPieceForSharding(hash) & shard_mask_];
        }

        void SetCapacity(size_t capacity) override {
            std::unique_lock<std::mutex> l(config_mutex_);
//            cout<<"global lru capacity: "<<capacity<<endl;
            capacity_ = capacity;
            auto per_shard = ComputePerShardCapacity(capacity);
            ForEachShard([=](CacheShard* cs) { cs->SetCapacity(per_shard); });
        }

        void SetStrictCapacityLimit(bool s_c_l) override {
            std::unique_lock<std::mutex> l(config_mutex_);
            strict_capacity_limit_ = s_c_l;
            ForEachShard(
                    [s_c_l](CacheShard* cs) { cs->SetStrictCapacityLimit(s_c_l); });
        }

        void Insert(const Slice& key, ObjectPtr obj,
                      size_t charge, Handle** handle = nullptr,
                      Priority priority = Priority::LOW) /*override*/ {
            HashVal hash = CacheShard::ComputeHash(key);
            auto h_out = reinterpret_cast<HandleImpl**>(handle);
            return GetShard(hash).Insert(key, hash, obj, charge, h_out,
                                         priority);
        }

//        Handle* CreateStandalone(const Slice& key, ObjectPtr obj,
//                                 const CacheItemHelper* helper, size_t charge,
//                                 bool allow_uncharged) override {
//            assert(helper);
//            HashVal hash = CacheShard::ComputeHash(key);
//            HandleImpl* result = GetShard(hash).CreateStandalone(
//                    key, hash, obj, helper, charge, allow_uncharged);
//            return reinterpret_cast<Handle*>(result);
//        }

//        Handle* Lookup(const Slice& key, /*const CacheItemHelper* helper = nullptr,
//                       CreateContext* create_context = nullptr,*/
//                       Priority priority = Priority::LOW,
//                       Statistics* stats = nullptr) override {
//            HashVal hash = CacheShard::ComputeHash(key);
//            HandleImpl* result = GetShard(hash).Lookup(key, hash, priority, stats);
//            return reinterpret_cast<Handle*>(result);
//        }

    std::pair<CompactStr, uint8_t> GetPhase1(const Slice &key)
    {
        HashVal hash = CacheShard::ComputeHash(key);
        auto result = GetShard(hash).GetPhase1(key, hash);
        return result;
    }

    bool GetPhase2(const Slice& key, CompactStr str)
    {
        HashVal hash = CacheShard::ComputeHash(key);
        return GetShard(hash).GetPhase2(key, hash, str);
    }

    void Print()
    {
        cout<<"the number of elements in each shard: ";
        for(int i = 0;i<GetNumShards();i++)
        {
            auto& shard = shards_[i];
            cout<<shard.GetOccupancyCount()<<' ';
        }
        cout<<endl;
    }

    /**
     *
     * @param key
     * @return first: true: success. false: this key's value is nullptr / this key do not exist.(the two things is the same because RDF is based on triple.)
     * @return second: need_evict.
     */
    std::pair<bool, bool> Del(unsigned int key, DiskIV& disk_iv)
    {
        auto key_ =   Slice((char*)(&key), sizeof(key));
        HashVal hash = CacheShard::ComputeHash(key_);
        return GetShard(hash).Del(key_,hash, disk_iv);
    }

    /**
     *
     * @return do we need to evict once again?
     */
    bool Set(unsigned key, CompactStr str)
    {
        auto key_ = Slice((char*)(&key), sizeof(key));
        HashVal hash = CacheShard::ComputeHash(key_);
        return GetShard(hash).Set(key_,hash,str);
   }


    std::vector<std::pair<void*, CompactStr>> EvictPhase1(unsigned key)
    {
          auto key_ =   Slice((char*)(&key), sizeof(key));
        HashVal hash = CacheShard::ComputeHash(key_);
        return GetShard(hash).EvictPhase1();
    }

    std::vector<std::pair<void*, CompactStr>> EvictUsingShardIdP1(unsigned shard_id)
    {
        return shards_[shard_id].EvictPhase1();
    }

    void Flush(DiskIV& disk_iv) {
        /// we do not need lock here. CacheManager::global_rw_lock ensures that nothing else is done in cache_manager when flushing
        for(int i = 0;i<GetNumShards();i++)
        {
            auto& shard = shards_[i];
            shard.Flush(disk_iv);
        }
    }
    /**
     *
     * @param key
     * @param evict_vec the vec of handle_ptr that has been written to disk.
     * @return do we need to evict once again?
     */
    bool EvictPhase2(unsigned key, const std::vector<void*>& evict_vec)
    {
        auto key_ = Slice((char*)(&key), sizeof(key));
        HashVal hash = CacheShard::ComputeHash(key_);
        return GetShard(hash).EvictPhase2(evict_vec);
    }

    bool EvictUsingShardIdP2(unsigned shard_id, const std::vector<void*>& evict_vec)
    {
        return shards_[shard_id].EvictPhase2(evict_vec);
    }


    protected:
    protected:
        inline void ForEachShard(const std::function<void(CacheShard*)>& fn) {
            uint32_t num_shards = GetNumShards();
            for (uint32_t i = 0; i < num_shards; i++) {
                fn(shards_ + i);
            }
        }

        inline size_t SumOverShards(
                const std::function<size_t(CacheShard&)>& fn) const {
            uint32_t num_shards = GetNumShards();
            size_t result = 0;
            for (uint32_t i = 0; i < num_shards; i++) {
                result += fn(shards_[i]);
            }
            return result;
        }

        inline size_t SumOverShards2(size_t (CacheShard::*fn)() const) const {
            return SumOverShards([fn](CacheShard& cs) { return (cs.*fn)(); });
        }

        // Must be called exactly once by derived class constructor
        void InitShards(const std::function<void(CacheShard*)>& placement_new) {
            ForEachShard(placement_new);
            destroy_shards_in_dtor_ = true;
        }

        void AppendPrintableOptions(std::string& str) const override {
            shards_[0].AppendPrintableOptions(str);
        }

    private:
        CacheShard* const shards_;
        bool destroy_shards_in_dtor_;
    };

// 512KB is traditional minimum shard size.
    int GetDefaultCacheShardBits(size_t capacity,
                                 size_t min_shard_size = 512U * 1024U);


